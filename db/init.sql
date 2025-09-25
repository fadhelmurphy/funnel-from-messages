-- rooms
CREATE TABLE IF NOT EXISTS rooms (
  id BIGSERIAL PRIMARY KEY,
  room_id TEXT UNIQUE NOT NULL,
  channel TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  raw_meta JSONB
);

-- messages (raw)
CREATE TABLE IF NOT EXISTS messages (
  id BIGSERIAL PRIMARY KEY,
  room_id BIGINT REFERENCES rooms(id),
  msg_id TEXT,
  sender_type TEXT,
  sender_id TEXT,
  phone TEXT,
  content TEXT,
  raw_payload JSONB,
  created_at TIMESTAMP WITH TIME ZONE
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_messages_msgid ON messages(msg_id) WHERE msg_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_room_created ON messages(room_id, created_at);

-- funnel table (output for growth)
CREATE TABLE IF NOT EXISTS funnel (
  id BIGSERIAL PRIMARY KEY,
  room_id BIGINT REFERENCES rooms(id),
  leads_date DATE,
  channel TEXT,
  phone TEXT,
  booking_date DATE,
  transaction_date DATE,
  transaction_value NUMERIC,
  opening_keyword TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_funnel_leadsdate ON funnel(leads_date);
