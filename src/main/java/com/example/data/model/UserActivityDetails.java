package com.example.data.model;

import com.example.data.util.DatabaseDetails;
import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Setter
@Getter
@DatabaseDetails(dbView = "USER_ACTIVITY_DETAILS",
        dbQuery = "SELECT * FROM USER_ACTIVITY_DETAILS WHERE ACTIVITY_TIME >= :fromTime AND ACTIVITY_TIME < :toTime AND UPPER(USER_USERID) IN ( :users )",
        s3Dir = "user_activity_details")
public class UserActivityDetails extends UserDto {
    private String userNameLast;
    private String userNameFirst;
    private String phoneNumber;
    private Timestamp deletedTime;
    private Timestamp activityTime;
    private String activityType;
    private String activityDetail;
}
