-- Спільні користувачі для Web і DPC. До ЛР3 X-User-Id містить id одного з них.
CREATE TABLE app_user (
  id   uuid PRIMARY KEY,
  name text NOT NULL,
  role text NOT NULL CHECK (role IN ('player', 'staff'))
);

-- гравці 00000000-0000-0000-0000-000000000001 … 00000000-0000-0000-0000-000000000010
INSERT INTO app_user (id, name, role)
SELECT ('00000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
       'player' || n, 'player'
FROM generate_series(1, 10) AS n;

-- організатор
INSERT INTO app_user (id, name, role) VALUES
  ('00000000-0000-0000-0000-000000000099', 'organizer', 'staff');
