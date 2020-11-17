import module namespace hep = "../common/hep.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)
  where $event.nMuon > 1
  where exists(
    for $muon1 in $event.muons[]
    for $muon2 in $event.muons[]
    where $muon1.idx < $muon2.idx
    where $muon1.charge != $muon2.charge
    let $invariantMass := hep:computeInvariantMass($muon1, $muon2)
    where 60 < $invariantMass and $invariantMass < 120
    return {}
  )
  return $event.MET_sumet
)

return hep:histogram($filtered, 0, 2000, 100)
