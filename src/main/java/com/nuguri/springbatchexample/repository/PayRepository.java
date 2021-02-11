package com.nuguri.springbatchexample.repository;

import com.nuguri.springbatchexample.entity.Pay;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PayRepository extends JpaRepository<Pay, Long> {
}
