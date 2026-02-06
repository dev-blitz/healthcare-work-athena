select
    *
from
    kickreason
where
                    -- kickreason.kickcode = ?
    kickreason.deleted is null
    and kickreason.kickreasoncategoryid = 1705;
