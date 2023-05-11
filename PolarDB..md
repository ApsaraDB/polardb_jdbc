## PolarDB 2.0 JDBC
基于社区42.5版本开发

## Release Notes
### Release 20230530
1 去$$符号功能
2 适配64位的DATE功能
3 完成所有的测试适配

### 测试
#### build.local.properties
host=11.239.69.226
server=11.239.69.226
port=59245
database=test
username=test
password=test
privilegedUser=postgres
privilegedPassword=postgres

#### 测试配置
host    all             all             0.0.0.0/0               md5
host    all             postgres        0.0.0.0/0               trust

#### 测试参数
polar_enable_stmt_transaction_rollback = off;
wal_level = logical
polar_default_with_rowid = off;
nls_timestamp_tz_format = "YYYY-MM-DD HH24:MI:SS.FFTZO"
