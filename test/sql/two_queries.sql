-- name: select-older_cats
-- Important to find old cats
SELECT * FROM cats
WHERE age > :age
ORDER BY age ASC

-- name:insert-cat
INSERT INTO cats (age)
VALUES (:age)
