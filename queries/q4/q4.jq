let $bucketWidth := (2000 - 0) div 100.0
let $bucketCenter := $bucketWidth div 2

let $loConst := round(-$bucketCenter div $bucketWidth)
let $hiConst := round((2000 - $bucketCenter) div $bucketWidth)

let $filtered := (
    for $i in parquet-file("/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet")
        let $subCount := sum(
            for $j in $i.Jet_pt[]
                return  if ($j > 40) then 1
                        else 0)
        where $subCount > 1
        return $i.MET_sumet)

for $i in $filtered
    let $binned := 
        if ($i < 0) then $loConst
        else
            if ($i < 2000) then round(($i - $bucketCenter) div $bucketWidth)
            else $hiConst
    let $x := $binned * $bucketWidth + $bucketCenter
    group by $x
    order by $x
    return {"x": $x, "y": count($i)}
