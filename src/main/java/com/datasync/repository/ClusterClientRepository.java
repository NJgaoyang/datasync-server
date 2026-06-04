package com.datasync.repository;

import com.datasync.entity.ClusterClient;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ClusterClientRepository extends JpaRepository<ClusterClient, Long> {
}
