<script setup lang="ts">
defineProps<{
  label: string;
  summary: string;
}>();
</script>

<template>
  <section class="section-placeholder">
    <p class="section-placeholder__eyebrow">MODULE</p>
    <h2>{{ label }}</h2>
    <p>{{ summary }}</p>
  </section>
</template>
