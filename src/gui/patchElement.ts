/** Preserve DOM identity and active gesture listeners while applying view changes. */
export function patchElement(current:Element,next:Element){
 for(const attribute of Array.from(current.attributes))if(!next.hasAttribute(attribute.name)&&!['data-bound','data-hit-scale'].includes(attribute.name))current.removeAttribute(attribute.name);
 for(const attribute of Array.from(next.attributes))if(current.getAttribute(attribute.name)!==attribute.value)current.setAttribute(attribute.name,attribute.value);
 const old=Array.from(current.childNodes),incoming=Array.from(next.childNodes);
 for(let i=0;i<incoming.length;i++){
  const a=old[i],b=incoming[i];if(!a){current.appendChild(b.cloneNode(true));continue;}
  if(a.nodeType!==b.nodeType||a.nodeType===1&&(a as Element).tagName!==(b as Element).tagName){a.replaceWith(b.cloneNode(true));continue;}
  if(a.nodeType===1)patchElement(a as Element,b as Element);else if(a.nodeValue!==b.nodeValue)a.nodeValue=b.nodeValue;
 }
 for(let i=incoming.length;i<old.length;i++)old[i].remove();
}
