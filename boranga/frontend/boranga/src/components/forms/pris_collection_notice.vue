<template>
    <alert v-if="text" type="primary">
        <div class="d-flex gap-3">
            <i
                class="bi bi-info-circle-fill text-primary fs-4 flex-shrink-0"
            ></i>
            <!-- eslint-disable-next-line vue/no-v-html -->
            <div class="notice-body" v-html="text"></div>
        </div>
    </alert>
    <alert v-else-if="fetchFailed" type="danger">
        The PRIS Collection Notice was unable to be retrieved.
    </alert>
</template>

<script>
import alert from '@vue-utils/alert.vue';
import { api_endpoints } from '@/utils/hooks.js';
export default {
    name: 'PrisCollectionNotice',
    components: { alert },
    data() {
        return {
            text: null,
            fetchFailed: false,
        };
    },
    mounted() {
        fetch(`${api_endpoints.help_text_entries}/pris_collection_notice/`)
            .then((r) => (r.ok ? r.json() : Promise.reject()))
            .then((data) => {
                this.text = data.text;
            })
            .catch(() => {
                this.fetchFailed = true;
            });
    },
};
</script>

<style scoped>
.notice-body :deep(p) {
    margin-bottom: 0;
}
</style>
