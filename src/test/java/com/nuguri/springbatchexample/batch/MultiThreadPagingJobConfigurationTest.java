package com.nuguri.springbatchexample.batch;

import com.nuguri.springbatchexample.config.MultiThreadPagingJobConfiguration;
import com.nuguri.springbatchexample.entity.Pay;
import com.nuguri.springbatchexample.entity.Pay2;
import com.nuguri.springbatchexample.repository.Pay2Repository;
import com.nuguri.springbatchexample.repository.PayRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {"chunkSize=1", "poolSize=2"})
public class MultiThreadPagingJobConfigurationTest {

    @Autowired
    @Qualifier(value = MultiThreadPagingJobConfiguration.JOB_NAME)
    private Job job;

    @Autowired
    private PayRepository payRepository;

    @Autowired
    private Pay2Repository pay2Repository;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void 페이징_분산처리() throws Exception {
        //given
        LocalDateTime txDateTime = LocalDateTime.of(2020, 1, 16, 0, 0, 0);
        List<Pay> pays = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            Pay pay = new Pay();
            pay.setAmount((long) i);
            pay.setTxDateTime(txDateTime);
            pay.setTxName("pay" + i);
            pays.add(pay);
        }
        payRepository.saveAll(pays);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("txDateTime", txDateTime.toString())
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.getJobLauncher().run(job, jobParameters);

        //then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        List<Pay2> pay2 = pay2Repository.findAll();
        pay2.sort(Comparator.comparingLong(Pay2::getAmount));

        assertThat(pay2).hasSize(10);
        assertThat(pay2.get(0).getAmount()).isEqualTo(0L);
        assertThat(pay2.get(9).getAmount()).isEqualTo(9L);
    }

}
