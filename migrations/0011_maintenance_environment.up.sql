-- Immutable identity binding a private database copy to one isolated
-- publisher/NATS deployment and its participant leases. Binding is explicit
-- (repair-wire-time --bind-environment); audit/apply never create this row.
CREATE TABLE IF NOT EXISTS maintenance_environment (
    singleton      boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    environment_id text NOT NULL CHECK (length(btrim(environment_id)) > 0),
    bound_at       timestamptz NOT NULL DEFAULT now()
);
