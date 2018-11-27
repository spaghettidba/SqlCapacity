CREATE VIEW [dbo].[Servers]
AS
WITH regservers
AS (
	SELECT CAST(N'/' + name AS nvarchar(255)) AS path ,*
	FROM msdb.dbo.sysmanagement_shared_server_groups AS g
),
serv AS (
	SELECT 0 as level, *
	FROM regservers AS s
	WHERE parent_id IS NULL

	UNION ALL

	SELECT s.level +1 as level, CAST(s.path + g.path AS nvarchar(255)) AS servpath
		,g.server_group_id
		,g.name
		,g.description
		,g.server_type
		,g.parent_id
		,g.is_system_object
		,g.num_server_group_children
		,g.num_registered_server_children
	FROM regservers AS g
	INNER JOIN serv AS s 
		ON g.parent_id = s.server_group_id
)
SELECT g.path, s.server_id, s.server_name
FROM msdb.dbo.sysmanagement_shared_registered_servers AS s
INNER JOIN serv AS g
	ON s.server_group_id = g.server_group_id;
