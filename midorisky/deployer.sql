create table Farms
(
    id          int           not null
        primary key,
    name        varchar(64)   not null,
    description varchar(128)  not null,
    lat         decimal(8, 6) null,
    lon         decimal(9, 6) null
);

create table IoTDeviceLogTest
(
    id              int auto_increment
        primary key,
    IoTType         varchar(50)  null,
    IoTSerialNumber varchar(20)  null,
    IoTStatus       tinyint      null,
    Timestamp       datetime(6)  null,
    PlotID          varchar(10)  null,
    ChangedBy       varchar(100) null
);

create table IoTDeviceLogs
(
    id              int auto_increment
        primary key,
    IoTType         varchar(50) not null,
    IoTSerialNumber varchar(20) not null,
    IoTStatus       tinyint     not null,
    Timestamp       datetime(6) not null,
    PlotID          varchar(10) not null
);

create table IoTDevices
(
    id              int auto_increment
        primary key,
    IoTType         varchar(50) null,
    IoTSerialNumber varchar(20) not null,
    IoTStatus       tinyint     null,
    PlotID          varchar(10) null,
    constraint IoTSerialNumber
        unique (IoTSerialNumber)
);

create table IoTDevicesTest
(
    id              int auto_increment
        primary key,
    IoTType         varchar(50)                        null,
    IoTStatus       tinyint                            null,
    IoTSerialNumber varchar(20)                        null,
    PlotID          varchar(10)                        null,
    LastDowntime    datetime                           null,
    LastUpdated     datetime default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    constraint IoTSerialNumber
        unique (IoTSerialNumber)
);

create table Notifications
(
    id         int auto_increment
        primary key,
    username   varchar(128)  default 'admin'                         not null,
    title      varchar(512)  default 'Sample Notification'           not null,
    subtitle   varchar(1024) default 'This is a sample notification' not null,
    action_url varchar(2048) default '/'                             not null,
    created_at datetime      default CURRENT_TIMESTAMP               not null,
    is_read    bit           default b'0'                            null,
    action     varchar(128)  default 'View'                          not null
);

create index Notifications_username_created_at_index
    on Notifications (username asc, created_at desc);

create table SoilMoistureIoT
(
    id              int auto_increment
        primary key,
    Timestamp       datetime(6) not null,
    PlotID          varchar(10) not null,
    IoTSerialNumber varchar(20) not null,
    SoilMoisture    double      not null
);

create table Tasks
(
    id          int auto_increment
        primary key,
    title       varchar(512)                       null,
    description longtext                           null,
    created_at  datetime default CURRENT_TIMESTAMP null,
    updated_at  datetime                           null,
    created_by  varchar(128)                       null,
    status      int      default 1                 null,
    priority    int      default 3                 null,
    hidden      bit      default b'0'              null
);

create table TaskComments
(
    id         int auto_increment
        primary key,
    comment    text                                   null,
    username   varchar(128) default 'admin'           null,
    taskId     int                                    null,
    created_at datetime     default CURRENT_TIMESTAMP null,
    constraint task_comments_Tasks_id_fk
        foreign key (taskId) references Tasks (id)
            on delete cascade
);

create table TasksAssignees
(
    id       int auto_increment
        primary key,
    username varchar(128) null,
    taskId   int          null,
    email    varchar(512) null,
    constraint fk_tasks
        foreign key (taskId) references Tasks (id)
            on delete cascade
);

create index fk_tasks_idx
    on TasksAssignees (taskId);

create table WeatherData
(
    id            int auto_increment
        primary key,
    Timestamp     datetime(6) not null,
    Windspeed     double      not null,
    Temperature   double      not null,
    Precipitation double      not null,
    Humidity      double      not null
);

create table WeatherIoT
(
    id              int auto_increment
        primary key,
    Timestamp       datetime(6) not null,
    PlotID          varchar(10) not null,
    IoTSerialNumber varchar(20) not null,
    Windspeed       double      not null,
    Temperature     double      not null,
    Precipitation   double      not null,
    Humidity        double      not null
);

create table WeatherPrediction
(
    id            int auto_increment
        primary key,
    Date          date        not null,
    Temperature   double      null,
    Humidity      double      null,
    Precipitation double      null,
    Windspeed     double      null,
    UID           varchar(36) not null
);

create table WeatherPrediction24
(
    id            int auto_increment
        primary key,
    DateTime      datetime    not null,
    Temperature   double      null,
    Humidity      double      null,
    Precipitation double      null,
    Windspeed     double      null,
    UID           varchar(36) not null
);

create index idx_datetime
    on WeatherPrediction24 (DateTime);

create table taskAttachments
(
    id       int auto_increment
        primary key,
    filename text not null,
    taskId   int  not null,
    constraint taskAttachments_tasks_id_fk
        foreign key (taskId) references Tasks (id)
            on delete cascade
);

create table wsConnections
(
    id            int auto_increment
        primary key,
    connection_id text not null,
    username      text not null
);

