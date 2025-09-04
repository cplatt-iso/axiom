#!/bin/bash
# Simple ILM Policy Management Script

API_KEY="9HXqH14f_IA59fdhJdDNpdLesTXz2CRVWCPfojxGs"
BASE_URL="https://axiom.trazen.org/api/v1/log-management"

show_policies() {
    echo "📋 Current Retention Policies:"
    curl -s -H "Authorization: Api-Key $API_KEY" \
        "$BASE_URL/retention-policies" | \
        jq '.[] | {id: .id, name: .name, pattern: .service_pattern, tier: .tier, delete_days: .delete_days}'
}

show_ilm_policies() {
    echo "🔄 Elasticsearch ILM Policies:"
    curl -s -H "Authorization: Api-Key $API_KEY" \
        "$BASE_URL/elasticsearch/ilm-policies" | \
        jq '.policies[] | select(startswith("axiom-"))'
}

sync_policies() {
    echo "🔄 Syncing policies to Elasticsearch..."
    curl -s -X POST -H "Authorization: Api-Key $API_KEY" \
        "$BASE_URL/elasticsearch/sync-policies" | jq
}

apply_templates() {
    echo "📝 Applying index templates..."
    curl -s -X POST -H "Authorization: Api-Key $API_KEY" \
        "$BASE_URL/elasticsearch/apply-templates" | jq
}

show_stats() {
    echo "📊 System Statistics:"
    curl -s -H "Authorization: Api-Key $API_KEY" \
        "$BASE_URL/statistics" | \
        jq '{cluster: .elasticsearch.cluster_name, indices: .axiom_indices.total_indices, docs: .axiom_indices.total_docs, size_gb: (.axiom_indices.total_size_bytes / 1024 / 1024 / 1024 | round)}'
}

case "$1" in
    "policies"|"p") show_policies ;;
    "ilm"|"i") show_ilm_policies ;;
    "sync"|"s") sync_policies ;;
    "templates"|"t") apply_templates ;;
    "stats"|"st") show_stats ;;
    "all"|"a") 
        show_policies
        echo ""
        show_ilm_policies
        echo ""
        show_stats
        ;;
    *)
        echo "Axiom Log Management CLI"
        echo "Usage: $0 [policies|ilm|sync|templates|stats|all]"
        echo ""
        echo "Commands:"
        echo "  policies (p)   - Show retention policies"
        echo "  ilm (i)        - Show Elasticsearch ILM policies"
        echo "  sync (s)       - Sync policies to Elasticsearch"
        echo "  templates (t)  - Apply index templates"
        echo "  stats (st)     - Show system statistics"
        echo "  all (a)        - Show everything"
        ;;
esac
