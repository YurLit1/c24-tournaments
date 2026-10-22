CREATE TABLE match_result_request (
  id         uuid        PRIMARY KEY,
  match_id   uuid        NOT NULL,
  winner_id  uuid        NOT NULL REFERENCES player_rating(player_id),
  loser_id   uuid        NOT NULL REFERENCES player_rating(player_id),
  state      text        NOT NULL CHECK (state IN ('queued', 'processing', 'done', 'rejected', 'failed')),
  reason     text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (winner_id <> loser_id)
);

CREATE TABLE match_result_applied (
  match_id   uuid        PRIMARY KEY,
  request_id uuid        NOT NULL REFERENCES match_result_request(id),
  applied_at timestamptz NOT NULL DEFAULT now()
);
