-- Business-operation cancellation barriers; no persisted runtime observations.
ALTER TABLE station.runtime_management_transitions
 DROP CONSTRAINT runtime_management_transitions_phase_check,
 DROP CONSTRAINT runtime_management_transitions_check;
ALTER TABLE station.runtime_management_transitions
 ADD CONSTRAINT runtime_management_transitions_phase_check CHECK
 (phase IN ('checking','sealing','canceling_source','canceled_source','applying',
 'verifying','prepared','restoring','restored','recovery_required',
 'releasing_source','releasing_target','released_source','released_target')),
 ADD CONSTRAINT runtime_management_transitions_check CHECK
 (phase IN ('checking','restored','releasing_source','released_source') OR checkpoint IS NOT NULL);
