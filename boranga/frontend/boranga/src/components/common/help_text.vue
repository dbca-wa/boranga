<template>
    <span>
        <template v-if="helpTextEntry">
            <template v-if="helpTextEntry.icon_with_popover">
                <i
                    ref="popoverIcon"
                    class="bi bi-info-circle-fill text-primary help-text-popover"
                    data-bs-toggle="popover"
                    data-bs-trigger="hover focus"
                    :data-bs-content="helpTextEntry.text"
                    data-bs-placement="top"
                ></i
                ><template v-if="helpTextEntry.user_can_administer">
                    <a
                        :href="`/admin/boranga/helptextentry/${helpTextEntry.id}/change/`"
                        role="button"
                        class="ms-2"
                        target="_blank"
                        title="Edit help text"
                        ><i class="bi bi-pencil-square"></i
                    ></a>
                </template>
            </template>
            <template v-else>
                <alert type="primary"
                    ><i
                        :id="helpTextEntry.section_id"
                        class="bi bi-info-circle-fill text-primary help-text-popover me-2"
                    ></i
                    >{{ helpTextEntry.text }}
                    <template v-if="helpTextEntry.user_can_administer">
                        <a
                            :href="`/admin/boranga/helptextentry/${helpTextEntry.id}/change/`"
                            role="button"
                            class="ms-2"
                            target="_blank"
                            title="Edit help text"
                            ><i class="bi bi-pencil-square"></i
                        ></a>
                    </template>
                </alert>
            </template>
        </template>
    </span>
</template>

<script>
import alert from '@vue-utils/alert.vue';
import { api_endpoints } from '@/utils/hooks.js';

export default {
    name: 'HelpText',
    components: {
        alert,
    },
    props: {
        section_id: {
            type: String,
            required: true,
        },
    },
    data: function () {
        return {
            helpTextEntry: null,
        };
    },
    mounted: function () {
        this.fetchHelpText();
    },
    methods: {
        fetchHelpText: function () {
            let vm = this;
            if (!vm.helpTextEntry) {
                fetch(
                    `${api_endpoints.help_text_entries}/${vm.section_id}/`
                ).then(async (response) => {
                    const data = await response.json();
                    if (!response.ok) {
                        return;
                    }
                    vm.helpTextEntry = data;
                    this.$nextTick(() => {
                        if (vm.helpTextEntry.icon_with_popover) {
                            new bootstrap.Popover(vm.$refs.popoverIcon, {
                                container: 'body',
                            });
                        }
                    });
                });
            }
        },
    },
};
</script>
