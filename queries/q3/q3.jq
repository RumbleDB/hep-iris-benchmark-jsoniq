let $bucketWidth := (60 - 15) div 100.0
let $bucketCenter := 0.375

let $loConst := round((15 - $bucketCenter) div $bucketWidth)
let $hiConst := round((60 - $bucketCenter) div $bucketWidth)

let $temp := (
	for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
	    let $pointFiltered := (
	    	for $j in size($i.Jet_pt)
		        where abs($i.Jet_eta[$j][]) < 1
		        return 	if ($i.Jet_pt[$j][] < 15) then $loConst
		        		else 
		        			if ($i.Jet_pt[$j][] < 60) then round(($i.Jet_pt[$j][] - $bucketCenter) div $bucketWidth)
		        			else $hiConst
		        		)
	    return $pointFiltered
	)

for $i in $temp
let $x := $i * $bucketWidth + $bucketCenter
group by $x
order by $x
return {"x": $x, "y": count($i)}