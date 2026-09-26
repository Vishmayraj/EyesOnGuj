-- BUG-018: Add department_id to persons_watchlist
ALTER TABLE persons_watchlist ADD COLUMN department_id UUID REFERENCES departments(id) ON DELETE SET NULL;
