declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

declare function buildHistogram($rawData, $histoConsts) {
    for $i in $rawData
    let $y := if ($i < $histoConsts.loBound) 
              then $histoConsts.loConst
              else
                if ($i < $histoConsts.hiBound)
                then round(($i - $histoConsts.center) div $histoConsts.width)
                else $histoConsts.hiConst
    let $x := $y * $histoConsts.width + $histoConsts.center
    group by $x
    order by $x
    return {"x": $x, "y": count($y)}
};

declare function histogramConsts($loBound, $hiBound, $binCount) {
	let $bucketWidth := ($hiBound - $loBound) div $binCount
	let $bucketCenter := $bucketWidth div 2

	let $loConst := round(($loBound - $bucketCenter) div $bucketWidth)
	let $hiConst := round(($hiBound - $bucketCenter) div $bucketWidth)

	return {"bins": $binCount, "width": $bucketWidth, "center": $bucketCenter, "loConst": $loConst, "hiConst": $hiConst,
			"loBound": $loBound, "hiBound": $hiBound}
};

let $histogram := histogramConsts(0, 2000, 100)

let $filtered := parquet-file($dataPath).MET_sumet

return buildHistogram($filtered, $histogram)
