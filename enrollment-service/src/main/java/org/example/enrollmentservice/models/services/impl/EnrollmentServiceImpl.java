package org.example.enrollmentservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.enrollmentservice.exceptions.DuplicateCourseException;
import org.example.enrollmentservice.models.constants.EnrollmentStatus;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentDetailRequest;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentRequest;
import org.example.enrollmentservice.models.dto.responses.CourseResponse;
import org.example.enrollmentservice.models.dto.responses.EnrollmentDetailResponse;
import org.example.enrollmentservice.models.dto.responses.EnrollmentResponse;
import org.example.enrollmentservice.models.entities.Enrollment;
import org.example.enrollmentservice.models.entities.EnrollmentDetail;
import org.example.enrollmentservice.models.repositories.EnrollmentDetailRepository;
import org.example.enrollmentservice.models.repositories.EnrollmentRepository;
import org.example.enrollmentservice.models.services.EnrollmentService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Service
@RequiredArgsConstructor
public class EnrollmentServiceImpl implements EnrollmentService {

    private final EnrollmentRepository enrollmentRepository;
    private final EnrollmentDetailRepository enrollmentDetailRepository;
    private final CourseGatewayService courseGatewayService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    @Transactional
    public EnrollmentResponse createEnrollment(CreateEnrollmentRequest request) {
        List<Long> courseIds = request.items().stream()
                .map(CreateEnrollmentDetailRequest::courseId)
                .toList();

        if (new HashSet<>(courseIds).size() != courseIds.size()) {
            throw new DuplicateCourseException();
        }

        Enrollment enrollment = new Enrollment();
        enrollment.setStudentName(request.studentName());
        enrollment.setStudentEmail(request.studentEmail());
        enrollment.setStatus(EnrollmentStatus.PENDING);

        Enrollment savedEnrollment = enrollmentRepository.save(enrollment);

        List<EnrollmentDetailResponse> detailResponses = new ArrayList<>();
        double totalFee = 0.0;

        for (CreateEnrollmentDetailRequest item : request.items()) {
            CourseResponse course = courseGatewayService.getCourseById(item.courseId());
            double courseFee = course.courseFee() == null ? 0.0 : course.courseFee();
            totalFee += courseFee;

            EnrollmentDetail detail = EnrollmentDetail.builder()
                    .enrollment(savedEnrollment)
                    .courseId(course.courseId())
                    .courseFee(courseFee)
                    .build();

            EnrollmentDetail savedDetail = enrollmentDetailRepository.save(detail);
            detailResponses.add(new EnrollmentDetailResponse(
                    savedDetail.getId(),
                    course.courseId(),
                    course.courseName(),
                    courseFee,
                    courseFee
            ));
        }

        savedEnrollment.setTotalFee(totalFee);
        savedEnrollment = enrollmentRepository.save(savedEnrollment);

        kafkaTemplate.send("enrollment-created", savedEnrollment.getStudentEmail());

        return new EnrollmentResponse(
                savedEnrollment.getId(),
                savedEnrollment.getStudentName(),
                savedEnrollment.getStudentEmail(),
                savedEnrollment.getTotalFee(),
                savedEnrollment.getStatus(),
                detailResponses
        );
    }
}
