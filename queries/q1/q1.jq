let $bucketWidth := (2000 - 0) div 100.0
let $bucketCenter := $bucketWidth div 2

let $loConst := round(-$bucketCenter div $bucketWidth)
let $hiConst := round((2000 - $bucketCenter) div $bucketWidth)

for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
	let $filtered := 
	    if ($i.MET_sumet lt 0) then $loConst
	    else
	        if ($i.MET_sumet lt 2000) then round(($i.MET_sumet - $bucketCenter) div $bucketWidth)
	        else $hiConst
	let $x := $filtered * $bucketWidth + $bucketCenter
	group by $x
	order by $x
	return {"x": $x, "y": count($i)}