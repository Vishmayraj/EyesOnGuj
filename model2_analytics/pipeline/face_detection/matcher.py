"""
Generic Face Detection & Person Watchlist Matching Engine.
==========================================================
Processes individual video frames or live stream feeds:
  1. Detects all faces using YuNet (cv2.FaceDetectorYN).
  2. Extracts 512-d unit-normalized biometric embeddings using InceptionResnetV1.
  3. Queries PostgreSQL pgvector for nearest active watchlist suspect using cosine distance.
  4. If match is within threshold (cosine distance <= 0.30, similarity >= 0.70):
       - Persists match face crop to disk.
       - Inserts an alert record into `person_alerts`.
       - Returns matched suspect details and similarity score.
"""

import hashlib
import logging
import os
import threading
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import cv2
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from pipeline.faceembedding.encoder import FaceEmbeddingEngine

logger = logging.getLogger("sentinel.face_detection.matcher")
logger.setLevel(logging.INFO)

# Verified YuNet Model Config
DEFAULT_WEIGHTS_DIR = Path(__file__).resolve().parents[2] / "weights"
YUNET_MODEL_URL = (
    "https://github.com/opencv/opencv_zoo/raw/main/models/face_detection_yunet/face_detection_yunet_2023mar.onnx"
)
YUNET_MODEL_NAME = "face_detection_yunet_2023mar.onnx"
YUNET_SHA256 = "8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4"

# Match crops directory
DEFAULT_MATCH_CROPS_DIR = Path(__file__).resolve().parents[2] / "uploads" / "face_matches"
DEFAULT_MATCH_CROPS_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class FaceDetectionResult:
    """Detection & matching outcome for a single detected face in a frame."""
    bbox: Tuple[int, int, int, int]  # (x, y, w, h)
    confidence: float
    landmarks: np.ndarray             # (5, 2) array
    is_match: bool = False
    person_id: Optional[str] = None
    person_name: Optional[str] = None
    category: Optional[str] = None
    similarity_score: float = 0.0
    distance: float = 1.0
    photo_path: Optional[str] = None
    face_crop_path: Optional[str] = None
    alert_id: Optional[str] = None


class FaceMatchEngine:
    """
    Generic, decoupled face detection and person watchlist matcher.
    Accepts raw video frames from pre-recorded videos or live RTSP streams.
    """

    def __init__(
        self,
        weights_dir: Optional[Path] = None,
        match_crops_dir: Optional[Path] = None,
        db_session_factory: Optional[Callable[[], Session]] = None,
        cosine_distance_threshold: float = 0.30,
        min_face_confidence: float = 0.60,
        min_face_size: int = 36,
    ):
        self.weights_dir = Path(weights_dir) if weights_dir else DEFAULT_WEIGHTS_DIR
        self.weights_dir.mkdir(parents=True, exist_ok=True)
        self.yunet_path = self.weights_dir / YUNET_MODEL_NAME

        self.match_crops_dir = Path(match_crops_dir) if match_crops_dir else DEFAULT_MATCH_CROPS_DIR
        self.match_crops_dir.mkdir(parents=True, exist_ok=True)

        self.db_session_factory = db_session_factory
        self.threshold = cosine_distance_threshold
        self.min_confidence = min_face_confidence
        self.min_face_size = min_face_size

        self._detector_lock = threading.Lock()
        self._detector: Optional[cv2.FaceDetectorYN] = None
        self._init_detector()

        self.encoder = FaceEmbeddingEngine(weights_dir=self.weights_dir)

    def _init_detector(self) -> None:
        """Download (if missing), verify cryptographic hash, and initialize FaceDetectorYN."""
        if self.yunet_path.exists():
            content = self.yunet_path.read_bytes()
            if hashlib.sha256(content).hexdigest() != YUNET_SHA256:
                logger.warning("Corrupted YuNet weights detected. Removing for fresh download.")
                self.yunet_path.unlink(missing_ok=True)

        if not self.yunet_path.exists():
            tmp_path = self.yunet_path.with_suffix(".onnx.tmp")
            try:
                logger.info(f"Downloading YuNet face detection weights to {tmp_path}...")
                urllib.request.urlretrieve(YUNET_MODEL_URL, str(tmp_path))
                if hashlib.sha256(tmp_path.read_bytes()).hexdigest() != YUNET_SHA256:
                    tmp_path.unlink(missing_ok=True)
                    raise RuntimeError("Downloaded YuNet weights failed SHA-256 integrity check.")
                tmp_path.replace(self.yunet_path)
            except Exception as e:
                tmp_path.unlink(missing_ok=True)
                raise RuntimeError(f"Failed to initialize YuNet weights: {e}") from e

        if hasattr(cv2, "FaceDetectorYN"):
            self._detector = cv2.FaceDetectorYN.create(
                model=str(self.yunet_path),
                config="",
                input_size=(320, 320),
                score_threshold=self.min_confidence,
                nms_threshold=0.3,
                top_k=5000,
            )
            logger.info("YuNet face detector initialized successfully.")
        else:
            raise RuntimeError("cv2.FaceDetectorYN is unavailable in installed OpenCV.")

    def detect_faces(self, frame: np.ndarray) -> List[Tuple[np.ndarray, float, np.ndarray]]:
        """
        Run YuNet face detection on an input BGR frame.
        Returns: list of (bbox, confidence, landmarks_5x2).
        """
        if frame is None or frame.size == 0 or self._detector is None:
            return []

        h, w = frame.shape[:2]
        with self._detector_lock:
            self._detector.setInputSize((w, h))
            _, raw_faces = self._detector.detect(frame)

        if raw_faces is None or len(raw_faces) == 0:
            return []

        results = []
        for f in raw_faces:
            score = float(f[14])
            if score < self.min_confidence:
                continue

            x = max(0, int(f[0]))
            y = max(0, int(f[1]))
            box_w = min(w - x, int(f[2]))
            box_h = min(h - y, int(f[3]))

            if box_w < self.min_face_size or box_h < self.min_face_size:
                continue

            bbox = np.array([x, y, box_w, box_h], dtype=int)
            landmarks = f[4:14].reshape((5, 2))
            results.append((bbox, score, landmarks))

        return results

    def query_watchlist_match(
        self,
        embedding: List[float],
        db: Session,
    ) -> Optional[Dict[str, Any]]:
        """
        Query PostgreSQL pgvector HNSW index for the closest active watchlist suspect.
        Returns match dict if cosine distance <= self.threshold, else None.
        """
        emb_str = "[" + ",".join(f"{x:.6f}" for x in embedding) + "]"

        # Cosine distance operator <=> returns 1 - cosine_similarity for normalized vectors
        query = text("""
            SELECT id, name, category, photo_path, (face_embedding <=> CAST(:emb AS vector)) AS distance
            FROM persons_watchlist
            WHERE status = 'active' AND face_embedding IS NOT NULL
            ORDER BY face_embedding <=> CAST(:emb AS vector) ASC
            LIMIT 1;
        """)

        row = db.execute(query, {"emb": emb_str}).fetchone()
        if not row:
            return None

        distance = float(row.distance)
        if distance <= self.threshold:
            similarity = round(max(0.0, min(1.0, 1.0 - distance)), 4)
            return {
                "id": str(row.id),
                "name": row.name,
                "category": row.category,
                "photo_path": row.photo_path,
                "distance": round(distance, 4),
                "similarity": similarity,
            }

        return None

    def process_frame(
        self,
        frame: np.ndarray,
        camera_id: Optional[str] = "prerecorded",
        frame_timestamp: Optional[datetime] = None,
        db: Optional[Session] = None,
    ) -> List[FaceDetectionResult]:
        """
        Processes a single video frame: detects faces, extracts embeddings, matches against pgvector,
        and saves alerts if a match is found.
        """
        detected_faces = self.detect_faces(frame)
        if not detected_faces:
            return []

        timestamp = frame_timestamp or datetime.now(timezone.utc)
        results: List[FaceDetectionResult] = []

        # Use passed session or create one if session factory is provided
        close_session = False
        active_db = db
        if active_db is None and self.db_session_factory:
            active_db = self.db_session_factory()
            close_session = True

        try:
            h, w = frame.shape[:2]
            for bbox, score, landmarks in detected_faces:
                x, y, bw, bh = bbox

                # Crop face with small margin for thumbnail
                pad_x = int(bw * 0.15)
                pad_y = int(bh * 0.15)
                x1 = max(0, x - pad_x)
                y1 = max(0, y - pad_y)
                x2 = min(w, x + bw + pad_x)
                y2 = min(h, y + bh + pad_y)
                face_crop = frame[y1:y2, x1:x2].copy()

                # Extract 512-d embedding using full oriented frame and original detector landmarks
                try:
                    embedding = self.encoder.extract_embedding(frame, landmarks=landmarks, crop_face=True)
                except Exception as e:
                    logger.debug(f"Failed to extract face embedding: {e}")
                    results.append(FaceDetectionResult(
                        bbox=(x, y, bw, bh),
                        confidence=score,
                        landmarks=landmarks,
                        is_match=False,
                    ))
                    continue

                # Query pgvector for watchlist match
                match_info = None
                if active_db:
                    try:
                        match_info = self.query_watchlist_match(embedding, active_db)
                    except Exception as e:
                        logger.warning(f"Watchlist pgvector query error: {e}")

                if match_info:
                    alert_id = uuid.uuid4()
                    crop_filename = f"{alert_id}.jpg"
                    crop_dest = self.match_crops_dir / crop_filename
                    cv2.imwrite(str(crop_dest), face_crop)
                    rel_crop_path = f"/api/v1/face-detection/crops/{crop_filename}"

                    # Insert into person_alerts table (camera_id is UUID FK, NULL for prerecorded)
                    resolved_cam_id = str(camera_id) if camera_id else None
                    if active_db:
                        try:
                            insert_query = text("""
                                INSERT INTO person_alerts (
                                    id, person_id, camera_id, similarity_score, distance,
                                    face_crop_path, frame_timestamp, created_at
                                ) VALUES (
                                    :id, :person_id, :camera_id, :similarity_score, :distance,
                                    :face_crop_path, :frame_timestamp, now()
                                );
                            """)
                            active_db.execute(insert_query, {
                                "id": alert_id,
                                "person_id": uuid.UUID(match_info["id"]),
                                "camera_id": resolved_cam_id,
                                "similarity_score": match_info["similarity"],
                                "distance": match_info["distance"],
                                "face_crop_path": rel_crop_path,
                                "frame_timestamp": timestamp,
                            })
                            active_db.commit()
                        except Exception as e:
                            logger.error(f"Failed to persist person_alert: {e}")
                            active_db.rollback()

                    results.append(FaceDetectionResult(
                        bbox=(x, y, bw, bh),
                        confidence=score,
                        landmarks=landmarks,
                        is_match=True,
                        person_id=match_info["id"],
                        person_name=match_info["name"],
                        category=match_info["category"],
                        similarity_score=match_info["similarity"],
                        distance=match_info["distance"],
                        photo_path=match_info["photo_path"],
                        face_crop_path=rel_crop_path,
                        alert_id=str(alert_id),
                    ))
                else:
                    results.append(FaceDetectionResult(
                        bbox=(x, y, bw, bh),
                        confidence=score,
                        landmarks=landmarks,
                        is_match=False,
                    ))
        finally:
            if close_session and active_db:
                active_db.close()

        return results

    def draw_overlays(
        self,
        frame: np.ndarray,
        detections: List[FaceDetectionResult],
    ) -> np.ndarray:
        """
        Renders high-visibility surveillance bounding boxes, facial landmarks,
        and alert badges on the video frame.
        """
        out = frame.copy()

        # Render top HUD banner
        matches_count = sum(1 for d in detections if d.is_match)
        hud_bg_color = (20, 20, 180) if matches_count > 0 else (20, 30, 40)
        cv2.rectangle(out, (10, 10), (380, 42), hud_bg_color, -1)
        cv2.rectangle(out, (10, 10), (380, 42), (0, 255, 255) if matches_count > 0 else (100, 110, 120), 1)
        hud_text = f"LIVE AI FEED | FACES: {len(detections)}"
        if matches_count > 0:
            hud_text += f" | ALERT: {matches_count} SUSPECT!"
        cv2.putText(
            out, hud_text, (18, 32), cv2.FONT_HERSHEY_SIMPLEX, 0.52,
            (255, 255, 255), 1, cv2.LINE_AA
        )

        for det in detections:
            x, y, w, h = det.bbox

            if det.is_match:
                cat = (det.category or "").lower()
                color = (0, 170, 255) if cat == "missing" else (30, 30, 245)  # BGR Red / Orange
                label = f"{det.person_name} ({int(det.similarity_score * 100)}%)"
                tag = f"[{cat.upper()} MATCH]"
            else:
                color = (255, 220, 0)  # BGR Cyan
                label = f"Face {int(det.confidence * 100)}%"
                tag = "[DETECTED]"

            # Draw glowing bounding box
            cv2.rectangle(out, (x, y), (x + w, y + h), color, 2)

            # Draw tech corner brackets
            line_len = max(10, int(min(w, h) * 0.2))
            thick = 3
            # Top-left
            cv2.line(out, (x, y), (x + line_len, y), color, thick)
            cv2.line(out, (x, y), (x, y + line_len), color, thick)
            # Top-right
            cv2.line(out, (x + w, y), (x + w - line_len, y), color, thick)
            cv2.line(out, (x + w, y), (x + w, y + line_len), color, thick)
            # Bottom-left
            cv2.line(out, (x, y + h), (x + line_len, y + h), color, thick)
            cv2.line(out, (x, y + h), (x, y + h - line_len), color, thick)
            # Bottom-right
            cv2.line(out, (x + w, y + h), (x + w - line_len, y + h), color, thick)
            cv2.line(out, (x + w, y + h), (x + w, y + h - line_len), color, thick)

            # Draw facial landmark points
            if det.landmarks is not None and len(det.landmarks) > 0:
                for lx, ly in det.landmarks:
                    cv2.circle(out, (int(lx), int(ly)), 2, (0, 255, 200), -1, cv2.LINE_AA)

            # Draw badge background
            full_text = f"{tag} {label}".strip()
            font = cv2.FONT_HERSHEY_SIMPLEX
            scale = 0.48
            thickness = 1
            (tw, th), baseline = cv2.getTextSize(full_text, font, scale, thickness)

            badge_y1 = max(0, y - th - 8)
            badge_y2 = y
            badge_x1 = x
            badge_x2 = min(out.shape[1], x + tw + 10)

            # Filled badge
            cv2.rectangle(out, (badge_x1, badge_y1), (badge_x2, badge_y2), color, -1)
            # Text on badge
            text_color = (0, 0, 0) if (not det.is_match or cat == "missing") else (255, 255, 255)
            cv2.putText(
                out,
                full_text,
                (badge_x1 + 4, badge_y2 - 4),
                font,
                scale,
                text_color,
                thickness,
                cv2.LINE_AA,
            )

        return out
