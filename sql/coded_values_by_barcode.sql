/*
 * Copyright © 2015 Merrimack Valley Library Consortium
 * Jason Stephenson <jstephenson@mvlc.org>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */
-- This script looks up the circ_modifier, circ_as_type, and coded
-- value list that might affect circ and hold matrix matchpoints for a
-- copy given the barcode.  It's handy if you're debugging hold and
-- circ policy, etc.
SELECT acp.barcode, acp.circ_modifier, acp.circ_as_type,
string_agg(DISTINCT ccvm.ctype || ':' || ccvm.code, ', ')
FROM asset.copy acp
JOIN asset.call_number acn ON acp.call_number = acn.id
JOIN metabib.record_attr_vector_list mravl ON acn.record = mravl.source
JOIN config.coded_value_map ccvm ON idx(mravl.vlist, ccvm.id) > 0
AND ccvm.ctype in ('item_type', 'bib_level', 'vr_format', 'item_form')
WHERE acp.barcode = :'barcode'
AND NOT acp.deleted
GROUP BY 1,2,3
