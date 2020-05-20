declare function histogramConsts($loBound, $hiBound, $binCount) {
    let $bucketWidth := ($hiBound - $loBound) div $binCount
    let $bucketCenter := $bucketWidth div 2

    let $loConst := round(($loBound - $bucketCenter) div $bucketWidth)
    let $hiConst := round(($hiBound - $bucketCenter) div $bucketWidth)

    return {"bins": $binCount, "width": $bucketWidth, "center": $bucketCenter, "loConst": $loConst, "hiConst": $hiConst,
            "loBound": $loBound, "hiBound": $hiBound}
};

let $dataPath := "/home/dan/data/garbage/git/rumble-root-queries/data/Run2012B_SingleMu_small.parquet"
let $histogram := histogramConsts(0, 2000, 100)


let $filtered := (
    for $i in parquet-file($dataPath)
    let $subCount := sum(
        for $j in $i.Jet_pt[]
        return  if ($j > 40) 
                then 1
                else 0
        )
    where $subCount > 1
    return $i.MET_sumet
)


for $i in $filtered
let $y :=   if ($i < $histogram.loBound) 
            then $histogram.loConst
            else
                if ($i < $histogram.hiBound)
                then round(($i - $histogram.center) div $histogram.width)
                else $histogram.hiConst
let $x := $y * $histogram.width + $histogram.center
group by $x
order by $x
return {"x": $x, "y": count($i)}
