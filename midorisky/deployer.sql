create table Farms
(
    id          int           not null
        primary key,
    name        varchar(64)   not null,
    description varchar(128)  not null,
    lat         decimal(8, 6) null,
    lon         decimal(9, 6) null
);

create table IoT
(
    id              int auto_increment
        primary key,
    IoTType         varchar(50) not null,
    IoTSerialNumber varchar(20) not null,
    IoTStatus       tinyint     not null,
    Timestamp       datetime(6) not null,
    PlotID          varchar(10) not null
);

create table IoTDevice
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
    IoTSerialNumber varchar(20) null,
    IoTStatus       tinyint     null,
    PlotID          varchar(10) null
);

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

create table WeatherSensor
(
    id            int auto_increment
        primary key,
    Timestamp     datetime(6) not null,
    Windspeed     double      not null,
    Temperature   double      not null,
    Precipitation double      not null,
    Humidity      double      not null
);

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

