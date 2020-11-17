import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)
  where $event.nMuon > 1
  where exists(
    for $muon1 at $i in $event.muons[]
    for $muon2 at $j in $event.muons[]
    where $i < $j
    where $muon1.charge != $muon2.charge
    let $invariantMass := hep:computeInvariantMass($muon1, $muon2)
    where 60 < $invariantMass and $invariantMass < 120
    return {}
  )
  return $event.MET_sumet
)

return hep:histogram($filtered, 0, 2000, 100)
