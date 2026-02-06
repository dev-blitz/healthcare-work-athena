SELECT *
FROM claimnote
WHERE
created >= DATEADD(YEAR, -1, GETDATE())
AND
kickreasonid = 20938 limit 5;
