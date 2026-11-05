/**
 * Resolve screen-space taps independently of SVG paint order. Invisible 44px
 * targets overlap when zoomed out: the nearest tank or placement + must win,
 * not whichever transparent circle was painted last.
 *
 * Self-contained so browser tests can execute this exact production resolver
 * with locator.evaluate(), rather than maintaining a second hit-test policy.
 */
export function resolveMapPointers(svg: SVGSVGElement, points: {x:number;y:number}[]): (number|null)[] {
  const centres = Array.from(svg.querySelectorAll<SVGGraphicsElement>('.territory-hit,.placement-plus-hit')).flatMap(hit => {
    const territory = hit.closest('[data-territory]');
    if (!territory || !hit.getClientRects().length) return [];
    const rect = hit.getBoundingClientRect();
    const id = Number(territory.getAttribute('data-territory'));
    return Number.isInteger(id) ? [{id, x:rect.x+rect.width/2, y:rect.y+rect.height/2}] : [];
  });
  return points.map(({x,y}) => {
    const top = svg.ownerDocument.elementFromPoint(x,y);
    // A menu, combat panel or clipped/off-screen point must remain unreachable.
    if (!top || !svg.contains(top)) return null;
    let nearest:number|null = null, distance = 22;
    for (const centre of centres) {
      const d = Math.hypot(x-centre.x,y-centre.y);
      if (d <= 22 && (nearest === null || d < distance)) {
        nearest = centre.id;
        distance = d;
      }
    }
    if (nearest !== null) return nearest;
    // Outside touch slop, a tap on the actual land keeps its natural target.
    const territory = top.closest('[data-territory]');
    return territory ? Number(territory.getAttribute('data-territory')) : null;
  });
}
