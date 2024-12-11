-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT MAX(era) FROM pitching;
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear FROM people WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  select namefirst, namelast, birthyear FROM people AS p WHERE p.namefirst LIKE '% %' ORDER BY namefirst, namelast;
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*) FROM people GROUP BY birthyear ORDER BY birthyear;
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*) FROM people GROUP BY birthyear HAVING AVG(height) > 70 ORDER BY birthyear;
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, people.playerid, yearid
  FROM people, HallOfFame
  WHERE people.playerid = HallOfFame.playerid 
  AND inducted = 'Y'
  ORDER BY yearid DESC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, p.playerid, s.schoolid, yearid
  FROM people AS p, HallOfFame AS h, collegeplaying AS c, schools AS s
  WHERE
    p.playerid = h.playerid AND
    h.inducted = 'Y' AND
    p.playerid = c.playerid AND
    c.schoolid = s.schoolid AND
    s.schoolState = "CA"
  ORDER BY yearid DESC, s.schoolid, p.playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT p.playerid, namefirst, namelast, c.schoolid 
  FROM people AS p
    INNER JOIN HallOfFame AS h ON p.playerid = h.playerid
    LEFT OUTER JOIN collegeplaying AS c ON p.playerid = c.playerid
  WHERE
    h.inducted = 'Y'
  ORDER BY p.playerid DESC, c.schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerid, namefirst, namelast, yearid, 
    ((b.H + b.H2B + 2 * b.H3B + 3 * b.HR) / CAST(b.AB AS FLOAT)) as slg
  FROM people AS p
  INNER JOIN batting AS b ON p.playerid = b.playerid
  WHERE b.AB > 50
  ORDER BY slg DESC
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, namefirst, namelast,
    (SUM(b.H) + SUM(b.H2B) + 2 * SUM(b.H3B) + 3 * SUM(b.HR)) / (CAST(SUM(b.AB) AS FLOAT)) AS lslg
  FROM people AS p
  INNER JOIN batting AS b ON p.playerid = b.playerid
  GROUP BY p.playerid
  HAVING SUM(b.AB) > 50
  ORDER BY lslg DESC
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  WITH may AS (
    SELECT 
    (SUM(b.H) + SUM(b.H2B) + 2 * SUM(b.H3B) + 3 * SUM(b.HR)) / (CAST(SUM(b.AB) AS FLOAT)) AS lslg
    FROM people AS p
    INNER JOIN batting AS b ON p.playerid = b.playerid
    GROUP BY p.playerid
    HAVING p.playerid = "mayswi01"
  ),
  PlayerState AS (
    SELECT namefirst, namelast,
      (SUM(b.H) + SUM(b.H2B) + 2 * SUM(b.H3B) + 3 * SUM(b.HR)) / (CAST(SUM(b.AB) AS FLOAT)) AS lslg,
      SUM(b.AB) AS AB
    FROM people AS p
    INNER JOIN batting AS b ON p.playerid = b.playerid
    GROUP BY p.playerid, p.namefirst, p.namelast
  )
  SELECT p.namefirst, p.namelast, p.lslg
  FROM PlayerState AS p
  INNER JOIN may
  WHERE p.AB > 50 AND p.lslg > may.lslg
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary)
  FROM salaries
  GROUP BY yearid 
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH data_state AS (
    SELECT 
      MIN(salary) AS min_value,
      MAX(salary) AS max_value
    FROM salaries
    WHERE yearid = "2016"
  ), bin_calculation AS (
    SELECT
    min_value,
    max_value,
    (max_value - min_value) / 10.0 AS bin_width
    FROM data_state
  ), bin_range AS (
    SELECT
    binid,
    (binid * bin_width + min_value) AS bin_start,
    ((binid + 1) * bin_width + min_value) AS bin_end
    FROM bin_calculation
    INNER JOIN binids
  )
  SELECT 
  binid,
  bin_start,
  bin_end,
  COUNT(*)
  FROM bin_range
  LEFT JOIN salaries
  ON 
  salary >= bin_start AND
  ((binid < 9 AND salary < bin_end) OR
  (binid = 9 AND salary <= bin_end))
  
  WHERE yearid = "2016"
  GROUP BY binid, bin_start, bin_end
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH prev_year AS (
    SELECT
    yearid + 1 AS yearid,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary,
    AVG(salary) AS avg_salary
    FROM salaries
    GROUP BY yearid
    ORDER BY yearid
  ), year AS (
    SELECT 
    yearid,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary,
    AVG(salary) AS avg_salary
    FROM salaries
    GROUP BY yearid
    ORDER BY yearid
  )
  SELECT y.yearid,
  y.min_salary - py.min_salary,
  y.max_salary - py.max_salary,
  y.avg_salary - py.avg_salary
  FROM year y
  INNER JOIN prev_year py
  ON y.yearid = py.yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  select salaries.playerid, people.namefirst, people.namelast, max(salary), yearid
  from salaries
  inner join people
  on people.playerid = salaries.playerid
  where yearid = "2000" OR yearid = "2001"
  group by yearid
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  select a.teamid, max(s.salary) - min(s.salary)
  from allstarfull as a
  inner join salaries as s
  on s.playerid = a.playerid AND s.yearid = a.yearid
  where a.yearid = "2016"
  group by a.teamid
;

