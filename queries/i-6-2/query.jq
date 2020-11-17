import module namespace hep = "../common/hep.jq";
import module namespace i-6 = "../i-6/common.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered :=
  for $event in parquet-file($dataPath)
  where $event.nJet > 2
  let $min-triplet-idxs := i-6:find-min-triplet-idx($event)

  return max(
    for $i in $min-triplet-idxs[]
    return $event.Jet_btag[[$i]]
  )

return hep:histogram($filtered, 0, 1, 100)
