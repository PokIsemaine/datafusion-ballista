SELECT AVG(sub.l_quantity) AS median_quantity
FROM (
    SELECT l_quantity,
           ROW_NUMBER() OVER (ORDER BY l_quantity) AS rn,
           COUNT(*) OVER () AS total_count
    FROM lineitem
) sub
WHERE sub.rn IN (
    FLOOR((total_count + 1) / 2),
    CEIL((total_count + 1) / 2)
);