-- crosswalk_init/init_crosswalk.sql

-- Ensure database exists (specified in docker-compose env)
-- CREATE DATABASE IF NOT EXISTS crosswalk_data;
-- USE crosswalk_data; -- Switch to the database

-- Create the crosswalk table
CREATE TABLE IF NOT EXISTS patient_id_crosswalk (
    old_mrn VARCHAR(50) PRIMARY KEY NOT NULL, -- Old MRN is the primary key, must be unique
    new_mrn VARCHAR(50) NOT NULL,            -- New MRN
    patient_name VARCHAR(255),               -- Optional: Patient name for reference
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Track changes
);

-- Add an index to the column we'll likely replace with
CREATE INDEX idx_new_mrn ON patient_id_crosswalk (new_mrn);

-- Insert some sample data
INSERT INTO patient_id_crosswalk (old_mrn, new_mrn, patient_name) VALUES
('MRN001', 'SITEA-1001', 'Doe^John'),
('MRN002', 'SITEA-1002', 'Smith^Jane'),
('12345', 'SITEB-98765', 'Jones^Bob'),
('ABC-789', 'SITEC-XYZ1', 'Williams^Mary'),
('MRN_Needs_Fix', 'SITEA-1005', 'Test^Patient')
ON DUPLICATE KEY UPDATE -- Avoid errors if script runs multiple times
    new_mrn = VALUES(new_mrn),
    patient_name = VALUES(patient_name);

-- You can add more tables or data here if needed
