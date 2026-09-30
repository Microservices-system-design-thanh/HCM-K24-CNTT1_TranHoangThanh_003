package org.example.enrollmentservice.models.services.impl;

import org.example.enrollmentservice.models.constants.EnrollmentStatus;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentDetailRequest;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentRequest;
import org.example.enrollmentservice.models.dto.responses.CourseResponse;
import org.example.enrollmentservice.models.dto.responses.EnrollmentResponse;
import org.example.enrollmentservice.models.entities.Enrollment;
import org.example.enrollmentservice.models.repositories.EnrollmentDetailRepository;
import org.example.enrollmentservice.models.repositories.EnrollmentRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnrollmentServiceImplTest {

    @Mock
    private EnrollmentRepository enrollmentRepository;

    @Mock
    private EnrollmentDetailRepository enrollmentDetailRepository;

    @Mock
    private CourseGatewayService courseGatewayService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private EnrollmentServiceImpl enrollmentService;

    @Test
    void shouldCreateEnrollmentSuccessfully() {
        when(courseGatewayService.getCourseById(1L)).thenReturn(new CourseResponse(1L, "Java Core", 100.0));
        when(courseGatewayService.getCourseById(2L)).thenReturn(new CourseResponse(2L, "Spring Boot", 150.0));
        when(enrollmentRepository.save(any(Enrollment.class))).thenAnswer(invocation -> {
            Enrollment enrollment = invocation.getArgument(0);
            enrollment.setId(10L);
            return enrollment;
        });
        when(enrollmentDetailRepository.saveAll(anyList())).thenAnswer(invocation -> invocation.getArgument(0));
        when(enrollmentDetailRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        CreateEnrollmentRequest request = new CreateEnrollmentRequest(
                "Nguyen Van A",
                "nguyenvana@example.com",
                List.of(
                        new CreateEnrollmentDetailRequest(1L),
                        new CreateEnrollmentDetailRequest(2L)
                )
        );

        EnrollmentResponse response = enrollmentService.createEnrollment(request);

        assertEquals("Nguyen Van A", response.studentName());
        assertEquals("nguyenvana@example.com", response.studentEmail());
        assertEquals(250.0, response.totalFee());
        assertEquals(EnrollmentStatus.PENDING, response.status());
        assertEquals(2, response.items().size());
        verify(kafkaTemplate).send("enrollment-created", "nguyenvana@example.com");
    }
}
