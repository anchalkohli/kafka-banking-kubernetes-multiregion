package com.example.replay.repository;

import com.example.replay.model.ReplayCommand;
import com.example.replay.model.ReplayJob;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ReplayJobRepository {
    private final JdbcTemplate jdbc;

    public ReplayJobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public ReplayJob create(String region, ReplayCommand command, String requestedBy) {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        jdbc.update("""
                insert into replay_job(id, region, partition_no, start_offset, end_offset_exclusive, next_offset,
                                       max_records, records_per_second, reason, incident_id, requested_by,
                                       status, replayed_count, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING_APPROVAL', 0, ?, ?)
                """,
                id, region, command.partition(), command.startOffset(), command.endOffsetExclusive(), command.startOffset(),
                command.maxRecords(), command.recordsPerSecond(), command.reason(), command.incidentId(), requestedBy,
                Timestamp.from(now), Timestamp.from(now));
        return find(id).orElseThrow();
    }

    public Optional<ReplayJob> find(UUID id) {
        return jdbc.query("select * from replay_job where id = ?", rs -> {
            if (!rs.next()) return Optional.empty();
            Timestamp approvedAt = rs.getTimestamp("approved_at");
            return Optional.of(new ReplayJob(
                    rs.getObject("id", UUID.class), rs.getString("region"), rs.getInt("partition_no"),
                    rs.getLong("start_offset"), rs.getLong("end_offset_exclusive"), rs.getLong("next_offset"),
                    rs.getInt("max_records"), rs.getInt("records_per_second"), rs.getString("reason"),
                    rs.getString("incident_id"), rs.getString("requested_by"), rs.getString("approved_by"),
                    approvedAt == null ? null : approvedAt.toInstant(), rs.getString("status"),
                    rs.getInt("replayed_count"), rs.getString("error_message"),
                    rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant()));
        }, id);
    }

    public boolean approve(UUID id, String approvedBy) {
        return jdbc.update("""
                update replay_job
                   set status='APPROVED', approved_by=?, approved_at=now(), error_message=null, updated_at=now()
                 where id=? and status='PENDING_APPROVAL'
                """, approvedBy, id) == 1;
    }

    public boolean markRunningIfApproved(UUID id) {
        return jdbc.update("""
                update replay_job
                   set status='RUNNING', error_message=null, updated_at=now()
                 where id=? and status='APPROVED'
                """, id) == 1;
    }

    public void checkpoint(UUID id, long nextOffset, int replayedCount) {
        jdbc.update("update replay_job set next_offset=?, replayed_count=?, updated_at=now() where id=?",
                nextOffset, replayedCount, id);
    }

    public void complete(UUID id, long nextOffset, int replayedCount) {
        jdbc.update("update replay_job set status='COMPLETED', next_offset=?, replayed_count=?, updated_at=now() where id=?",
                nextOffset, replayedCount, id);
    }

    public void fail(UUID id, long nextOffset, int replayedCount, String errorMessage) {
        String safe = errorMessage == null ? "Replay failed" : errorMessage.substring(0, Math.min(errorMessage.length(), 1000));
        jdbc.update("update replay_job set status='FAILED', next_offset=?, replayed_count=?, error_message=?, updated_at=now() where id=?",
                nextOffset, replayedCount, safe, id);
    }
}
