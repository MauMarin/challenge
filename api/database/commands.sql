-- ***** Ejercicio 1 *****
-- Se facilita la ejecución mediante el uso de CTEs
-- Uno que selecciona cada una de las tres tablas
-- Como se requiere un enfoque en la tabla de empleados para hacer los contadores, este CTE los obtiene por aparte
-- Se utiliza un WHERE para reducir la cantidad de datos obtenidos y asi incrementar la eficiencia del query
-- Se usan USE-CASES para definir cada uno de los cuatrimestres del año
WITH 
    emp_cte AS (
        SELECT 
            e.*, 
            YEAR(e.hire_ts) as year_c, 
            QUARTER(e.hire_ts) as hired_q 
        FROM hired_employees e
        WHERE year_c = 2021
    ),
    department_cte AS (
        SELECT * FROM departments
    ),
    job_cte AS (
        SELECT * FROM jobs
    )

SELECT 
    d.department.name, 
    j.job.name, 
    COUNT(CASE WHEN e.hired_q = 1 THEN 1 END) AS Q1, 
    COUNT(CASE WHEN e.hired_q = 2 THEN 1 END) AS Q2, 
    COUNT(CASE WHEN e.hired_q = 3 THEN 1 END) AS Q3,
    COUNT(CASE WHEN e.hired_q = 4 THEN 1 END) AS Q4
FROM emp_cte e 
INNER JOIN job_cte j ON j.id = e.job 
INNER JOIN department_cte d ON d.id = e.department 
GROUP BY d.department, j.job 
ORDER BY d.department, j.job;


 q