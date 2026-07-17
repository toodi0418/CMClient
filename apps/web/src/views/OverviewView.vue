<template>
  <section class="overview-grid" aria-label="控制總覽">
    <div class="signal-board">
      <div class="signal-board__header">
        <span>GATEWAY SIGNAL</span>
        <span class="signal-board__time">LOCAL</span>
      </div>
      <div class="signal-board__body">
        <strong>Awaiting live status</strong>
        <p>Gateway API 與即時事件會在連線建立後顯示。</p>
      </div>
    </div>
    <div class="overview-strip" aria-label="控制狀態">
      <div>
        <span>TRANSPORT</span>
        <strong>--</strong>
      </div>
      <div>
        <span>EVENT STREAM</span>
        <strong>--</strong>
      </div>
      <div>
        <span>ACTIVE JOBS</span>
        <strong>--</strong>
      </div>
    </div>
  </section>
</template>
