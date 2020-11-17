import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)

  let $subCount := count(
    for $jet in $event.jets[]
    where $jet.pt > 40
    return $jet
  )
  where $subCount > 1

  return $event.MET_sumet
)

return hep:histogram($filtered, 0, 2000, 100)
