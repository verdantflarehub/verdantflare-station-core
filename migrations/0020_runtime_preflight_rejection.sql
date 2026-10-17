-- A durable rejection before any checkpoint or workload mutation.
ALTER TABLE station.runtime_management_transitions
 DROP CONSTRAINT runtime_management_transitions_phase_check,
 DROP CONSTRAINT runtime_management_transitions_check;
ALTER TABLE station.runtime_management_transitions
 ADD CONSTRAINT runtime_management_transitions_phase_check CHECK
 (phase IN ('checking','rejected_preflight','sealing','canceling_source','canceled_source','applying',
 'verifying','prepared','restoring','restored','recovery_required',
 'releasing_source','releasing_target','released_source','released_target')),
 ADD CONSTRAINT runtime_management_transitions_check CHECK
 ((phase = 'rejected_preflight' AND checkpoint IS NULL) OR
 (phase <> 'rejected_preflight' AND
 (phase IN ('checking','restored','releasing_source','released_source') OR checkpoint IS NOT NULL)));
