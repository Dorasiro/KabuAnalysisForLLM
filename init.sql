-- =========================================
-- 初期化SQL Copilot改訂版
-- =========================================

-- 分類体系テーブル (TSE33, TOPIX17, GICS, ICBなど)
CREATE TABLE sector_classifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(20) NOT NULL UNIQUE, -- 例: 'TSE33', 'TOPIX17', 'GICS'
    description TEXT NULL
);

-- セクターテーブル (業種一覧)
CREATE TABLE sectors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,       -- 業種名
    sector_classification_id INT NOT NULL,
    FOREIGN KEY (sector_classification_id) REFERENCES sector_classifications(id),
    UNIQUE KEY uq_sectors (name, sector_classification_id)
);

-- 市場テーブル（東証プライム、NASDAQ、NYSEなど）
CREATE TABLE markets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE, -- 市場名は一意
	timezone VARCHAR(50) NOT NULL,
	open1 TIME NOT NULL,
	close1 TIME NOT NULL,
	open2 TIME NULL,
	close2 TIME NULL
);

-- 証券テーブル (銘柄マスタ)
CREATE TABLE securities (
    id INT AUTO_INCREMENT PRIMARY KEY,
    market_id INT NOT NULL,
    code VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    FOREIGN KEY (market_id) REFERENCES markets(id),
    UNIQUE KEY uq_securities (market_id, code)
);

-- 証券とセクターの関係 (多対多対応)
CREATE TABLE security_sectors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    security_id INT NOT NULL,
    sector_id INT NOT NULL,
    FOREIGN KEY (security_id) REFERENCES securities(id),
    FOREIGN KEY (sector_id) REFERENCES sectors(id),
    UNIQUE KEY uq_security_sectors (security_id, sector_id)
);

-- 株価テーブル (大量データ対応)
CREATE TABLE prices (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    security_id INT NOT NULL,
    date DATE NOT NULL,
    time TIME NULL,                   -- 日足以上なら NULL
    open DECIMAL(15,4),
    high DECIMAL(15,4),
    low DECIMAL(15,4),
    close DECIMAL(15,4),
    volume BIGINT,
    FOREIGN KEY (security_id) REFERENCES securities(id),
    UNIQUE KEY uq_prices (security_id, date, time)
);

-- 指数テーブル
CREATE TABLE indices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL
);

-- 指数構成テーブル (指数と銘柄の関係)
CREATE TABLE index_components (
    id INT AUTO_INCREMENT PRIMARY KEY,
    index_id INT NOT NULL,
    security_id INT NOT NULL,
    weight DECIMAL(10,4) NULL,
    FOREIGN KEY (index_id) REFERENCES indices(id),
    FOREIGN KEY (security_id) REFERENCES securities(id),
    UNIQUE KEY uq_index_components (index_id, security_id)
);

-- =========================================
-- 初期データ挿入
-- =========================================
-- セクター分類テーブル
INSERT INTO sector_classifications (code, description) VALUES
('TSE33', '日本株の標準的な業種分類'),
('TOPIX17', '東証33業種を17に集約した分類');

-- 市場テーブル初期データ
INSERT INTO markets (name, timezone, open1, close1, open2, close2) VALUES
('東証プライム', 'Asia/Tokyo', '09:00:00', '11:30:00', '12:30:00', '15:30:00'),
('東証スタンダード', 'Asia/Tokyo', '09:00:00', '11:30:00', '12:30:00', '15:30:00'),
('東証グロース', 'Asia/Tokyo', '09:00:00', '11:30:00', '12:30:00', '15:30:00');

-- セクターをファイルから取り込む
LOAD DATA LOCAL INFILE '/csv/TSE33.csv'
INTO TABLE sectors
CHARACTER SET utf8mb4
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(name)
SET sector_classification_id = (SELECT id FROM sector_classifications WHERE code='TSE33');

LOAD DATA LOCAL INFILE '/csv/TOPIX17.csv'
INTO TABLE sectors
CHARACTER SET utf8mb4
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(name)
SET sector_classification_id = (SELECT id FROM sector_classifications WHERE code='TOPIX17');

-- 証券テーブルをファイルから取り込む
-- 仮テーブル
CREATE TEMPORARY TABLE securities_csv (
    code VARCHAR(20),
    name VARCHAR(100),
    market_name VARCHAR(100),
    sector33_name VARCHAR(100),
    sector17_name VARCHAR(100)
);

-- CSVロード
LOAD DATA LOCAL INFILE '/csv/JPX_prime.csv'
INTO TABLE securities_csv
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- 証券テーブルへ
INSERT INTO securities (market_id, code, name)
SELECT m.id, c.code, c.name
FROM securities_csv c
JOIN markets m ON m.name = c.market_name;

-- 証券とセクター対応 (33業種)
INSERT INTO security_sectors (security_id, sector_id)
SELECT s.id, sec.id
FROM securities_csv c
JOIN securities s ON s.code = c.code
JOIN sectors sec ON sec.name = c.sector33_name
WHERE sec.sector_classification_id = (SELECT id FROM sector_classifications WHERE code='TSE33');

-- 証券とセクター対応 (17業種)
INSERT INTO security_sectors (security_id, sector_id)
SELECT s.id, sec.id
FROM securities_csv c
JOIN securities s ON s.code = c.code
JOIN sectors sec ON sec.name = c.sector17_name
WHERE sec.sector_classification_id = (SELECT id FROM sector_classifications WHERE code='TOPIX17');