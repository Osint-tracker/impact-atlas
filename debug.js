
const lines = [
  'Khurba Air Base',
  'https://ukr.warspotting.net/view/3074/6020/ Su-34 31-Red destroyed Borodianka, Bucha Raion 27/02/22',
  'https://ukr.warspotting.net/view/6197/71638/ Su-34M 51 Red shot down by friendly fire near Lotykove, Alchevske 18/07/22'
];

lines.forEach(line => {
  const urlMatch = line.match(/(https?:\/\/[^\s<>"']+)/i);
  const url = urlMatch ? urlMatch[1] : '';
  let txt = line.replace(/(https?:\/\/[^\s<>"']+)/gi, ' ').trim();
  const dateMatch = txt.match(/\b(20\d{2}-\d{1,2}-\d{1,2}|\d{1,2}[\/.\-]\d{1,2}[\/.\-]\d{2,4}|(?:19|20)\d{2})\b/);
  const dateTxt = dateMatch ? dateMatch[1] : 'Archive';
  if (dateMatch) txt = txt.replace(dateMatch[1], ' ').trim();
  txt = txt.replace(/^AND\s+/i, '').replace(/^[\-\u2022*]+\s*/, '').trim();
  
  // mock stripHtml
  const cleanTxt = (!txt || txt === 'N/A') ? 'Geolocation point' : txt;
  console.log('RESULT: >' + cleanTxt + '<');
});
