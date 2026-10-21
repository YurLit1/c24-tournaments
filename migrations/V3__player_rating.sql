CREATE TABLE player_rating (
  player_id uuid    PRIMARY KEY REFERENCES app_user(id),
  rating    integer NOT NULL,
  version   bigint  NOT NULL DEFAULT 0 CHECK (version >= 0)
);

INSERT INTO player_rating (player_id, rating)
SELECT id, 1500 FROM app_user WHERE role = 'player';
