(() => {
  const canvas = document.querySelector("#contact-sheet");
  if (!canvas) return;

  const random = (() => {
    let state = 0x8b7d2e41;
    return () => {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      return (state >>> 0) / 4294967296;
    };
  })();

  const frames = Array.from({ length: 18 }, (_, index) => ({
    x: random(), y: random(), width: 0.07 + random() * 0.16,
    height: 0.17 + random() * 0.34, exposure: .16 + random() * .22,
    offset: index * .37,
  }));
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function draw(now = 0) {
    const rect = canvas.getBoundingClientRect();
    const ratio = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.floor(rect.width * ratio);
    canvas.height = Math.floor(rect.height * ratio);
    const context = canvas.getContext("2d");
    context.scale(ratio, ratio);
    context.clearRect(0, 0, rect.width, rect.height);
    const beat = reducedMotion ? 0 : (Math.sin(now / 1750) + 1) * .5;

    context.strokeStyle = "rgba(184, 211, 74, .15)";
    context.lineWidth = 1;
    frames.forEach((frame) => {
      const x = Math.round(frame.x * rect.width);
      const y = Math.round((frame.y * .58 + .09) * rect.height);
      const w = Math.round(frame.width * rect.width);
      const h = Math.round(frame.height * rect.height);
      context.fillStyle = `rgba(231, 232, 227, ${frame.exposure + beat * .035})`;
      context.fillRect(x, y, w, h);
      context.strokeRect(x + .5, y + .5, w, h);
      context.fillStyle = "rgba(20, 24, 23, .38)";
      context.fillRect(x + 8, y + h - 16, Math.max(14, w * .28), 2);
    });

    context.strokeStyle = "rgba(231, 184, 100, .28)";
    frames.slice(0, 8).forEach((frame, index) => {
      const startX = (frame.x * rect.width) + 10;
      const startY = ((frame.y * .58 + .09) * rect.height) + 10;
      const endX = Math.min(rect.width - 14, startX + 70 + index * 13);
      const endY = Math.min(rect.height - 14, startY + 24);
      context.beginPath();
      context.moveTo(startX, startY);
      context.lineTo(endX, endY);
      context.stroke();
    });
    if (!reducedMotion) window.requestAnimationFrame(draw);
  }

  window.addEventListener("resize", () => draw(performance.now()));
  draw(performance.now());
})();
