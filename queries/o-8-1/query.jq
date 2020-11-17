import module namespace hep = "../common/hep.jq";
import module namespace o-8 = "../o-8/common.jq";
declare variable $dataPath as anyURI external := anyURI("../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in hep:RestructureDataParquet($dataPath)
  where integer($event.nMuon + $event.nElectron) > 2

  let $leptons := hep:ConcatLeptons($event)
  let $closest-lepton-pair := o-8:find-closest-lepton-pair($leptons)
  where exists($closest-lepton-pair)

  let $other-leption := (
    for $lepton at $i in $leptons
    where $i != $closest-lepton-pair.i and $i != $closest-lepton-pair.j
    order by $lepton.pt descending
    return $lepton
  )[1]

  return 2 * $event.MET_pt * $other-leption.pt *
    (1.0 - cos(hep:DeltaPhi($event.MET_phi, $other-leption.phi)))
)

return hep:histogram($filtered, 15, 250, 100)
