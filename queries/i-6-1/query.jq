import module namespace hep = "../common/hep.jq";
import module namespace i-6 = "../i-6/common.jq";
declare variable $input-path as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered :=
  for $event in parquet-file($input-path)
  where $event.nJet > 2
  let $min-triplet-idxs := i-6:find-min-triplet-idx($event)

  for $i in $min-triplet-idxs[]
  return $event.Jet_pt[[$i]]

return hep:histogram($filtered, 15, 40, 100)
