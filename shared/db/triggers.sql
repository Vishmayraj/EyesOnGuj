-- ============================================================
-- Sentinel — triggers
-- ============================================================
-- Two concerns:
--   1. Keep `cameras.updated_at` honest on every UPDATE.
--   2. Auto-write `status_history` rows for the fields Model 1's
--      audit trail actually cares about (Project_Context.md §3/§6),
--      instead of relying on every call site to remember to log it.
--
-- `changed_by` needs the acting user's id, which the DB can't know
-- on its own. The FastAPI layer sets it per-request/transaction with:
--
--     SET LOCAL app.current_user_id = '<uuid>';
--
-- before issuing the UPDATE. If it's never set (e.g. a bulk import
-- script, a console session), the trigger falls back to NULL rather
-- than erroring — `status_history.changed_by` already allows NULL,
-- and ON DELETE SET NULL there means a deleted user doesn't retroactively
-- break old audit rows either.
-- ============================================================

-- ------------------------------------------------------------
-- 1. updated_at bookkeeping
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cameras_set_updated_at
    BEFORE UPDATE ON cameras
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_vms_systems_set_updated_at
    BEFORE UPDATE ON vms_systems
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

-- ------------------------------------------------------------
-- 2. camera audit trail
-- ------------------------------------------------------------
-- Only logs the fields that matter for the registry audit trail —
-- not every column (e.g. grid_synced_at churns every poll cycle and
-- would flood status_history with noise nobody asked to audit).
CREATE OR REPLACE FUNCTION log_camera_changes()
RETURNS TRIGGER AS $$
DECLARE
    acting_user UUID;
BEGIN
    BEGIN
        acting_user := current_setting('app.current_user_id', true)::UUID;
    EXCEPTION WHEN OTHERS THEN
        acting_user := NULL;
    END;

    IF NEW.connectivity_status IS DISTINCT FROM OLD.connectivity_status THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'connectivity_status', OLD.connectivity_status, NEW.connectivity_status, acting_user);
    END IF;

    IF NEW.department_id IS DISTINCT FROM OLD.department_id THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'department_id', OLD.department_id::TEXT, NEW.department_id::TEXT, acting_user);
    END IF;

    IF NEW.district_id IS DISTINCT FROM OLD.district_id THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'district_id', OLD.district_id::TEXT, NEW.district_id::TEXT, acting_user);
    END IF;

    IF NEW.ownership IS DISTINCT FROM OLD.ownership THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'ownership', OLD.ownership, NEW.ownership, acting_user);
    END IF;

    IF NEW.storage_type IS DISTINCT FROM OLD.storage_type THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'storage_type', OLD.storage_type, NEW.storage_type, acting_user);
    END IF;

    IF NEW.retention_days IS DISTINCT FROM OLD.retention_days THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'retention_days', OLD.retention_days::TEXT, NEW.retention_days::TEXT, acting_user);
    END IF;

    IF NEW.vms_url IS DISTINCT FROM OLD.vms_url THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'vms_url', OLD.vms_url, NEW.vms_url, acting_user);
    END IF;

    IF NEW.is_active IS DISTINCT FROM OLD.is_active THEN
        INSERT INTO status_history (camera_id, changed_field, old_value, new_value, changed_by)
        VALUES (NEW.id, 'is_active', OLD.is_active::TEXT, NEW.is_active::TEXT, acting_user);
        IF NEW.is_active = false THEN
            NEW.decommissioned_at := now();
        ELSE
            NEW.decommissioned_at := NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cameras_log_changes
    BEFORE UPDATE ON cameras
    FOR EACH ROW
    EXECUTE FUNCTION log_camera_changes();

-- ------------------------------------------------------------
-- 3. block hard-deleting a department/district that's still in use
-- ------------------------------------------------------------
-- Redundant with the FK RESTRICT already on cameras.department_id,
-- but gives a readable error instead of a raw FK-violation message —
-- worth it since this is the exact mistake a bulk-cleanup script or
-- an admin in a hurry is likely to make.
CREATE OR REPLACE FUNCTION prevent_department_delete_if_in_use()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM cameras WHERE department_id = OLD.id) THEN
        RAISE EXCEPTION 'Cannot delete department %: cameras are still assigned to it. Reassign or deactivate them first.', OLD.name;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_departments_prevent_delete
    BEFORE DELETE ON departments
    FOR EACH ROW
    EXECUTE FUNCTION prevent_department_delete_if_in_use();