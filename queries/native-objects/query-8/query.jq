import module namespace hep = "../../common/hep.jq";
import module namespace query-8 = "../query-8-common/common.jq";
declare variable $input-path as anyURI external := anyURI("../../../data/Run2012B_SingleMu.root");

let $filtered := (
  for $event in parquet-file($input-path)
  where size($event.Muon) + size($event.Electron) > 2

  let $leptons := hep:concat-leptons($event)
  let $closest-lepton-pair := query-8:find-closest-lepton-pair($leptons)
  where exists($closest-lepton-pair)

  let $other-leption := (
    for $lepton at $i in $leptons
    where $i != $closest-lepton-pair.i and $i != $closest-lepton-pair.j
    order by $lepton.pt descending
    return $lepton
  )[1]

  return sqrt(2 * $event.MET.pt * $other-leption.pt *
    (1.0 - cos(hep:delta-phi($event.MET.phi, $other-leption.phi))))
)

return hep:histogram($filtered, 15, 250, 100)
