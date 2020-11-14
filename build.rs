// fn maven(s: &str, jvm: &Jvm) {
//     let artifact = MavenArtifact::from(s);

//     let _ = jvm.deploy_artifact(&artifact).map_err(|error| {
//         println!(
//             "cargo:warning=Could not download Maven artifact {}: {:?}",
//             s, error
//         );
//     });
// }

// fn deploy_java_artifacts(jvm: &Jvm) {
//     println!("cargo:warning=Downloading legacy Pepper modules...");
//     maven("org.corpus-tools:pepperModules-EXMARaLDAModules:1.3.1", jvm);

//     maven("org.apache.commons:commons-lang3:3.4", jvm);
//     maven("org.apache.felix:javax.servlet:1.0.0", jvm);
//     maven("org.corpus-tools:pepper-framework:3.3.3", jvm);
//     maven(
//         "org.eclipse.emf:org.eclipse.emf.ecore:2.9.1-v20130827-0309",
//         jvm,
//     );
//     maven("org.corpus-tools:exmaralda-emf-api:1.2.2", jvm);
//     maven("com.google.guava:guava:19.0", jvm);
//     maven("javax.xml.bind:jaxb-api-osgi:2.2.7", jvm);
//     maven("org.apache.felix:org.osgi.foundation:1.0.0", jvm);
//     maven("org.corpus-tools:salt-api:3.3.6", jvm);
//     maven("com.neovisionaries:nv-i18n:1.1", jvm);
//     maven(
//         "org.eclipse.emf:org.eclipse.emf.common:2.9.1-v20130827-0309",
//         jvm,
//     );
//     maven("com.fasterxml.woodstox:woodstox-core:5.0.3", jvm);
//     maven("com.sun.xml.bind:jaxb-osgi:2.2.7", jvm);
//     maven("org.knallgrau.utils:textcat:1.0.1", jvm);
//     maven("commons-io:commons-io:2.4", jvm);
//     maven(
//         "org.eclipse.emf:org.eclipse.emf.ecore.xmi:2.9.1-v20130827-0309",
//         jvm,
//     );
//     maven("org.json:json:20160810", jvm);
//     maven("org.codehaus.woodstox:stax2-api:3.1.4", jvm);
//     maven("org.slf4j:slf4j-api:1.7.5", jvm);
//     maven("com.sun.activation:javax.activation:1.2.0", jvm);
//     maven(
//         "org.eclipse.birt.runtime:org.eclipse.osgi.services:3.4.0.v20140312-2051",
//         jvm,
//     );

//     maven("org.apache.felix:org.osgi.core:1.0.0", jvm);
//     maven("org.apache.felix:org.osgi.compendium:1.0.0", jvm);
//     maven("org.assertj:assertj-core:2.4.1", jvm);
// }

fn main() {
    // TODO: download Pepper distribution and unzip its plugin folder to pepper-plugins
}
