-- 1. Create a dedicated Security Schema for administrative objects
CREATE SCHEMA Security;
GO

-- 2. Define the Inline Table-Valued Function (TVF) for the Security Predicate
-- SCHEMABINDING prevents the underlying mapping table from being accidentally dropped.
CREATE FUNCTION Security.fn_regional_auditor_predicate(@District AS VARCHAR(50))
    RETURNS TABLE
WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS fn_security_predicate_result
    FROM dbo.Security_Mapping
    WHERE AuditorEmail = USER_NAME() 
      AND AssignedDistrict = @District; 

-- 3. Apply the Security Policy to the Gold Fraud Alerts Table
CREATE SECURITY POLICY Security.Audit_Jurisdiction_Policy
ADD FILTER PREDICATE Security.fn_regional_auditor_predicate(District)
ON dbo.gold_pmla_alerts
WITH (STATE = ON);
GO

/* 
================================================================================================
VERIFICATION & TESTING
================================================================================================
*/

SELECT * FROM dbo.gold_pmla_alerts;