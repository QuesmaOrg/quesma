# Quesma EAP documentation

This folder contains our EAP documentation available at https://eap.quesma.com.
These docs are just static files generated with [Vitepress](https://vitepress.dev) and published via CloudFlare Pages.


### Contribute

Install Vitepress first:
```shell
npm add -D vitepress
```

Preview docs locally while editing:
```shell
npm run docs:dev
```


## Build locally & publish

Build the docs:
```shell
npm run docs:build
```
Above will build all the HTML assets in in `docs/.vitepress/dist`

You can preview what you've built with:
```shell
npm run docs:preview
```

And submit the PR :muscle:
CloudFlare Pages will pick up the PR and build a preview version of your changes.

Once merged, the changes will be automatically deployed to CloudFlare Pages (there's an integration set up which
deploys from `main` branch automatically).
