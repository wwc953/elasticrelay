-- MySQL 初始化脚本用于ElasticRelay CDC
-- 创建用户、数据库和必要的权限设置

-- 创建ElasticRelay专用用户（如果不存在）
CREATE USER IF NOT EXISTS 'elasticrelay_user'@'%' IDENTIFIED BY 'elasticrelay_pass';

-- 授予必要的权限
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'elasticrelay_user'@'%';
GRANT ALL PRIVILEGES ON `elasticrelay`.* TO 'elasticrelay_user'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 使用elasticrelay数据库
USE elasticrelay;

-- ========================================
-- 第一部分: 与 mysql_transform.json 规则匹配的测试表
-- 用于测试 transform 规则的完整功能
-- ========================================

-- users 表 (匹配 user-data-transform 规则)
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INT,
    phone VARCHAR(20),
    id_card VARCHAR(20),
    bank_card VARCHAR(25),
    password VARCHAR(255),
    address TEXT,
    status ENUM('active', 'inactive', 'deleted') DEFAULT 'active',
    balance DECIMAL(12,2) DEFAULT 0.00,
    is_vip BOOLEAN DEFAULT FALSE,
    is_test BOOLEAN DEFAULT FALSE,
    internal_notes TEXT,
    debug_info TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_status (status),
    INDEX idx_user_name (user_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- orders 表 (匹配 order-data-transform 规则)
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    order_no VARCHAR(50) UNIQUE NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    quantity INT DEFAULT 1,
    order_date DATE,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded') DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_order_no (order_no),
    INDEX idx_status (status),
    INDEX idx_order_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- audit_logs 表 (匹配 log-data-transform 规则)
CREATE TABLE IF NOT EXISTS audit_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    level ENUM('debug', 'info', 'warn', 'error') DEFAULT 'info',
    message TEXT,
    user_ip VARCHAR(45),
    request_body TEXT,
    response_body TEXT,
    INDEX idx_timestamp (timestamp),
    INDEX idx_level (level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ========================================
-- 第二部分: MySQL CDC 专用测试表 (带 mysql_ 前缀)
-- 用于测试实际的 CDC binlog 同步场景
-- ========================================

-- mysql_users 表
CREATE TABLE IF NOT EXISTS mysql_users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    birth_date DATE,
    profile JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_verified BOOLEAN DEFAULT FALSE,
    last_login TIMESTAMP NULL,
    INDEX idx_email (email),
    INDEX idx_created_at (created_at),
    INDEX idx_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS mysql_orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    order_number VARCHAR(50) UNIQUE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
    order_data JSON,
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    shipped_at TIMESTAMP NULL,
    delivered_at TIMESTAMP NULL,
    FOREIGN KEY (user_id) REFERENCES mysql_users(id) ON DELETE SET NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at),
    INDEX idx_status (status),
    INDEX idx_order_number (order_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS mysql_products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(100),
    specifications JSON,
    in_stock BOOLEAN DEFAULT TRUE,
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_sku (sku),
    INDEX idx_name (name),
    INDEX idx_price (price)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    metadata JSON,
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_created_at (created_at),
    INDEX idx_email (email),
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ========================================
-- 插入测试数据 - Transform 规则匹配表
-- ========================================

-- users 表测试数据 (用于 user-data-transform 规则测试)
INSERT IGNORE INTO users (user_name, email, first_name, last_name, age, phone, id_card, bank_card, password, address, status, balance, is_vip, is_test) VALUES 
('zhangsan', 'zhangsan@example.com', '三', '张', 28, '13812345678', '110101199001011234', '6222021234567890123', 'password123', '北京市朝阳区建国路100号', 'active', 1500.50, TRUE, FALSE),
('lisi', 'lisi@example.com', '四', '李', 35, '13987654321', '310101198501015678', '6222029876543210987', 'securepass', '上海市浦东新区陆家嘴200号', 'active', 3200.00, TRUE, FALSE),
('wangwu', 'wangwu@example.com', '五', '王', 22, '15011112222', '440101200001019012', '6222023456789012345', 'mypassword', '广州市天河区天河路300号', 'inactive', 100.00, FALSE, FALSE),
('test_user', 'test@test.com', 'Test', 'User', 99, '10000000000', '000000000000000000', '0000000000000000000', 'test', '测试地址', 'active', 0.00, FALSE, TRUE),
('deleted_user', 'deleted@example.com', 'Deleted', 'User', 40, '13333333333', '111111111111111111', '1111111111111111111', 'deleted', '已删除地址', 'deleted', 0.00, FALSE, FALSE);

-- orders 表测试数据 (用于 order-data-transform 规则测试)
INSERT IGNORE INTO orders (user_id, order_no, amount, quantity, order_date, status) VALUES 
(1, 'ORD-2024-001', 299.00, 2, '2024-01-15', 'pending'),
(1, 'ORD-2024-002', 1599.00, 1, '2024-01-20', 'shipped'),
(2, 'ORD-2024-003', 89.90, 5, '2024-02-01', 'delivered'),
(2, 'ORD-2024-004', 4999.00, 1, '2024-02-10', 'cancelled'),
(3, 'ORD-2024-005', 199.00, 3, '2024-02-15', 'refunded'),
(3, 'ORD-2024-006', 599.00, 1, '2024-03-01', 'processing');

-- audit_logs 表测试数据 (用于 log-data-transform 规则测试)
INSERT IGNORE INTO audit_logs (level, message, user_ip, request_body, response_body) VALUES 
('info', '用户登录成功', '192.168.1.100', '{"username": "zhangsan"}', '{"status": "success"}'),
('warn', '登录尝试失败', '10.0.0.50', '{"username": "unknown"}', '{"error": "invalid credentials"}'),
('error', '数据库连接超时', '172.16.0.1', NULL, '{"error": "connection timeout"}'),
('debug', '缓存刷新完成', '127.0.0.1', '{"cache_key": "user_list"}', '{"refreshed": true}');

-- ========================================
-- 插入测试数据 - MySQL CDC 专用表
-- ========================================

-- mysql_users 表测试数据
INSERT IGNORE INTO mysql_users (username, email, first_name, last_name, phone, profile) VALUES 
('zhangsan_mysql', 'zhangsan.mysql@test.com', '三', '张', '13800138001', JSON_OBJECT('level', 'vip', 'points', 1000, 'preferences', JSON_ARRAY('tech', 'gaming'))),
('lisi_mysql', 'lisi.mysql@test.com', '四', '李', '13800138002', JSON_OBJECT('level', 'premium', 'points', 2500, 'preferences', JSON_ARRAY('fashion', 'travel'))),
('wangwu_mysql', 'wangwu.mysql@test.com', '五', '王', '13800138003', JSON_OBJECT('level', 'basic', 'points', 100, 'preferences', JSON_ARRAY('sports', 'music')));

INSERT IGNORE INTO mysql_products (sku, name, description, price, category, specifications, stock_quantity) VALUES 
('MYSQL001', 'iPhone 15 Pro (MySQL)', 'Apple iPhone 15 Pro 256GB - MySQL Test', 8999.00, 'electronics', JSON_OBJECT('color', 'titanium', 'storage', '256GB', 'warranty', '1year'), 50),
('MYSQL002', 'MacBook Pro (MySQL)', 'Apple MacBook Pro 14inch M3 - MySQL Test', 15999.00, 'computers', JSON_OBJECT('processor', 'M3', 'ram', '16GB', 'storage', '512GB'), 20),
('MYSQL003', 'AirPods Pro (MySQL)', 'Apple AirPods Pro 3rd Gen - MySQL Test', 1899.00, 'accessories', JSON_OBJECT('noise_cancelling', true, 'battery_life', '6h', 'color', 'white'), 100);

INSERT IGNORE INTO test_table (name, email, age, metadata) VALUES 
('MySQL张三', 'mysql.zhangsan@example.com', 25, JSON_OBJECT('city', '北京', 'department', 'MySQL技术部', 'database', 'mysql')),
('MySQL李四', 'mysql.lisi@example.com', 30, JSON_OBJECT('city', '上海', 'department', 'MySQL产品部', 'database', 'mysql')),
('MySQL王五', 'mysql.wangwu@example.com', 28, JSON_OBJECT('city', '广州', 'department', 'MySQL运营部', 'database', 'mysql'));

-- 插入订单数据（引用用户）
INSERT IGNORE INTO mysql_orders (user_id, order_number, total_amount, status, order_data, shipping_address) 
SELECT 
    u.id,
    CONCAT('ORD-MYSQL-', LPAD(u.id, 6, '0')),
    8999.00,
    'pending',
    JSON_OBJECT(
        'items', JSON_ARRAY(
            JSON_OBJECT('sku', 'MYSQL001', 'quantity', 1, 'price', 8999.00)
        ),
        'payment_method', 'credit_card',
        'notes', 'MySQL CDC测试订单'
    ),
    CONCAT('MySQL测试地址 ', u.id, '号')
FROM mysql_users u
WHERE u.username LIKE '%mysql%'
LIMIT 3;

-- 创建存储过程用于测试CDC
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS GenerateTestData(IN record_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE random_name VARCHAR(100);
    DECLARE random_email VARCHAR(255);
    
    WHILE i <= record_count DO
        SET random_name = CONCAT('TestUser_', i, '_', UNIX_TIMESTAMP());
        SET random_email = CONCAT('test_', i, '_', UNIX_TIMESTAMP(), '@mysql-cdc.com');
        
        INSERT INTO test_table (name, email, age, metadata) VALUES (
            random_name,
            random_email,
            FLOOR(RAND() * 50) + 18,
            JSON_OBJECT(
                'batch', 'generated',
                'sequence', i,
                'timestamp', UNIX_TIMESTAMP(),
                'database', 'mysql'
            )
        );
        
        SET i = i + 1;
    END WHILE;
END//
DELIMITER ;

-- 显示初始化完成信息
SELECT 'MySQL database initialized successfully for ElasticRelay CDC' as message;
SELECT 'Created user: elasticrelay_user' as message;
SELECT 'Created tables for Transform testing: users, orders, audit_logs' as message;
SELECT 'Created tables for CDC testing: mysql_users, mysql_orders, mysql_products, test_table' as message;
SELECT 'Inserted sample data for all tables' as message;
SELECT 'Ready for Transform and binlog CDC testing' as message;

-- 显示表统计信息
SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    CREATE_TIME
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'elasticrelay' 
    AND TABLE_NAME IN ('users', 'orders', 'audit_logs', 'mysql_users', 'mysql_orders', 'mysql_products', 'test_table');

-- 显示binlog状态
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'server_id';
