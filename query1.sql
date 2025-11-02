-- Example query with several optimizable parts
SELECT d.name, COUNT(*) AS cnt
FROM Dept d
JOIN Emp e ON e.dept_id = d.id
WHERE d.region = 'US' AND e.salary > 100000
GROUP BY d.name
HAVING COUNT(*) > 2
ORDER BY cnt DESC;
