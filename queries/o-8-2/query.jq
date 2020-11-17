import module namespace hep = "../common/hep.jq";
import module namespace o-8 = "../o-8/common.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)
  where integer($event.nMuon + $event.nElectron) > 2

  let $leptons := hep:ConcatLeptons($event)
  let $closest-lepton-pair := o-8:find-closest-lepton-pair($leptons)
  where exists($closest-lepton-pair)

  return max(
    for $lepton at $i in $leptons
    where $i != $closest-lepton-pair.i and $i != $closest-lepton-pair.j
    return $lepton.pt
  )
)

return hep:histogram($filtered, 15, 60, 100)
