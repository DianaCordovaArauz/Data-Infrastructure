-- Crea BD y usa
CREATE DATABASE IF NOT EXISTS cotopaxi
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;
USE cotopaxi;

-- Tabla de quintiles (join 1:1 por RowID)
DROP TABLE IF EXISTS quintil_por_rowid;
CREATE TABLE quintil_por_rowid (
  row_id           VARCHAR(40) NOT NULL,
  canton_id        INT NULL,
  quintil_pobreza  TINYINT NOT NULL,
  anio             SMALLINT NOT NULL DEFAULT 2024,
  PRIMARY KEY (row_id),
  KEY idx_canton (canton_id),
  CONSTRAINT chk_quintil CHECK (quintil_pobreza BETWEEN 1 AND 5)
) ENGINE=InnoDB;

-- Procedimiento para poblar exactamente 135,068 filas: Row0..Row135067
DELIMITER //
DROP PROCEDURE IF EXISTS poblar_quintiles //
CREATE PROCEDURE poblar_quintiles(IN total_filas INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  START TRANSACTION;
  WHILE i < total_filas DO
    INSERT INTO quintil_por_rowid (row_id, canton_id, quintil_pobreza, anio)
    VALUES (
      CONCAT('Row', i),              -- coincide con tu columna RowID del CSV
      1 + FLOOR(RAND() * 221),       -- 1..221 (simulado)
      1 + FLOOR(RAND() * 5),         -- 1..5  (simulado)
      2024
    );
    SET i = i + 1;
    IF (i % 10000) = 0 THEN COMMIT; START TRANSACTION; END IF;
  END WHILE;
  COMMIT;
END //
DELIMITER ;

-- Poblar 135,068 registros
CALL poblar_quintiles(135068);

-- Verificación rápida
SELECT COUNT(*) AS filas FROM quintil_por_rowid;
SQL
