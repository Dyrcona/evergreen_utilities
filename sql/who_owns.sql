/*
 * Copyright © 2015 Merrimack Valley Library Consortium
 * Jason Stephenson <jstephenson@mvlc.org>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */
-- Given a copy barcode, this script returns the bibliograpic record
-- id, the call number owning lib, copy circ lib, and barcode of all
-- not deleted copies on the bibliographic record.  It is useful
-- sometimes in looking into holds issues.
select acn1.record, aou.shortname as owning_lib, aou1.shortname as circ_lib,
acp1.barcode
from asset.copy acp
join asset.call_number acn
on acn.id = acp.call_number
join asset.call_number acn1
on acn1.record = acn.record
and not acn1.deleted
join actor.org_unit aou
on aou.id = acn1.owning_lib
join asset.copy acp1
on acp1.call_number = acn1.id
and not acp1.deleted
join actor.org_unit aou1
on aou1.id = acp1.circ_lib
where acp.barcode = :'barcode'
and not acp.deleted
