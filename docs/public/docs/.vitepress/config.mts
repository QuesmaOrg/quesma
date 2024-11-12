import { defineConfig } from 'vitepress'
import { withMermaid } from "vitepress-plugin-mermaid";

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Quesma",
  description: "Quesma Database Gateway Early Access Program",
  head: [['link', { rel: 'icon', href: 'favicon.ico' }]],
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    logo: {
      light: '/logo/quesma-logo-black-full-svg.svg',
      dark: '/logo/quesma-logo-white-full-svg.svg'
    },
    siteTitle: 'Docs',
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Getting started', link: '/eap-docs' },
      { text: 'Back to home page', link: 'https://quesma.com' }

    ],

    sidebar: [
      {
        items: [
          { text: 'Getting started', link: '/eap-docs',
            items: [
              { text: 'What is Quesma?', link: '/eap-docs' },
              { text: 'Quick start demo', link: '/quick-start' },
            ],
          },
          { text: 'Installation guide', link: '/installation',
            items: [
              { text: 'Transparent Elasticsearch proxy', link: '/example-1' },
              { text: 'Adding ClickHouse tables to existing Kibana/Elasticsearch ecosystem', link: '/example-2-1',
                items: [
                  {text: 'Adding Hydrolix tables to existing Kibana/Elasticsearch ecosystem', link:  '/example-2-1-hydro-specific'}
                ] },
              { text: 'Query ClickHouse tables as Elasticsearch indices', link: '/example-2-0-clickhouse-specific',
                items: [
                  { text: 'Query Hydrolix tables as Elasticsearch indices', link: '/example-2-0'}
                ]
              },
            ],
          },
          { text: 'Advanced configuration',
            items: [
              { text: 'Configuration primer', link: '/config-primer'},
              { text: 'Ingest', link: '/ingest' },
              { text: 'A/B testing', link: '/ab-testing' },
            ],
          },
          { text: 'Known limitations or unsupported functionalities', link: '/limitations' },
          { text: 'Miscellaneous', link: '/misc',
            items: [
              { text: 'Creating Kibana Data Views', link: '/adding-kibana-dataviews' }
            ]
          }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/QuesmaOrg' },
      { icon: 'youtube', link: 'https://www.youtube.com/@QuesmaOrg' }
    ],

    search: {
      provider: 'local'
    }
  },
  ignoreDeadLinks: [
    // ignore exact url
    '/2024-04-24/reference.tgz',
    '/2024-04-25/reference.tgz',
    '/2024-05-10/reference.tgz',
    '/2024-06-05/reference.tgz',
    '/2024-07-05/reference.tgz',
    // ignore all localhost links
    /^https?:\/\/localhost/
  ],
  // Integrate Mermaid plugin configuration
  ...withMermaid({
    mermaid: {
      // Mermaid configuration options
      // Refer https://mermaid.js.org/config/setup/modules/mermaidAPI.html#mermaidapi-configuration-defaults
    },
    mermaidPlugin: {
      class: "mermaid my-class", // Additional CSS classes for the parent container
    },
  }),
})

