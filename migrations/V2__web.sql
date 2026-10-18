-- Web, ЛР1 О1.2: турніри, реєстрації, матчі.
-- Гроші — bigint у копійках + валюта; моменти — timestamptz; назва зони — окремо.

CREATE TABLE tournament (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  game              text        NOT NULL CHECK (char_length(game) BETWEEN 1 AND 100),
  starts_at         timestamptz NOT NULL,
  time_zone         text        NOT NULL DEFAULT 'Europe/Kyiv',
  slots             integer     NOT NULL CHECK (slots > 0),
  available         integer     NOT NULL,
  fee_minor         bigint      NOT NULL CHECK (fee_minor >= 0),
  currency          char(3)     NOT NULL DEFAULT 'UAH' CHECK (currency ~ '^[A-Z]{3}$'),
  registration_open boolean     NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now(),
  -- інваріант 1: учасників не більше, ніж місць
  CHECK (available BETWEEN 0 AND slots)
);

CREATE TABLE registration (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       uuid        NOT NULL REFERENCES app_user(id),
  tournament_id uuid        NOT NULL REFERENCES tournament(id),
  nickname      text        NOT NULL CHECK (char_length(nickname) BETWEEN 3 AND 32),
  status        text        NOT NULL CHECK (status IN ('held', 'confirmed', 'expired', 'cancelled')),
  version       integer     NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  CHECK (expires_at > created_at)
);

-- у гравця не більше однієї активної реєстрації на турнір
CREATE UNIQUE INDEX registration_one_active
  ON registration (user_id, tournament_id) WHERE status IN ('held', 'confirmed');
-- нікнейм унікальний серед активних реєстрацій турніру (без урахування регістру)
CREATE UNIQUE INDEX registration_nickname_active
  ON registration (tournament_id, lower(nickname)) WHERE status IN ('held', 'confirmed');
-- для списку «мої реєстрації» (О1.5)
CREATE INDEX registration_user_list ON registration (user_id, created_at DESC, id DESC);

CREATE TABLE match (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tournament_id uuid    NOT NULL REFERENCES tournament(id),
  round         integer NOT NULL CHECK (round >= 1),
  a_reg_id      uuid    NOT NULL REFERENCES registration(id),
  b_reg_id      uuid    NOT NULL REFERENCES registration(id),
  winner_reg_id uuid    REFERENCES registration(id),
  status        text    NOT NULL DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'finished')),
  finished_at   timestamptz,
  version       integer NOT NULL DEFAULT 1 CHECK (version >= 1),
  CHECK (a_reg_id <> b_reg_id),
  CHECK (winner_reg_id IS NULL OR winner_reg_id IN (a_reg_id, b_reg_id)),
  -- інваріант 2: finished ⇔ є переможець і момент завершення
  CHECK ((status = 'finished') = (winner_reg_id IS NOT NULL AND finished_at IS NOT NULL))
);

-- тестові турніри; ті самі id у прикладах openapi.yaml
INSERT INTO tournament (id, game, starts_at, slots, available, fee_minor) VALUES
  ('10000000-0000-0000-0000-000000000001', 'Dota 2', '2027-06-05 12:00+03', 64, 64, 5000),
  ('10000000-0000-0000-0000-000000000002', 'CS2',    '2027-06-12 12:00+03',  2,  2,    0);
