ALTER TABLE match_result_applied
  ADD COLUMN delta   integer,
  ADD COLUMN awarded boolean NOT NULL DEFAULT false;

CREATE TABLE season_award (
  player_id  uuid        NOT NULL REFERENCES player_rating(player_id),
  season     text        NOT NULL CHECK (char_length(season) BETWEEN 1 AND 32),
  match_id   uuid        NOT NULL,
  request_id uuid        NOT NULL REFERENCES match_result_request(id),
  awarded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (player_id, season)
);
