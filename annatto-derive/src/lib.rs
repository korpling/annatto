use proc_macro::TokenStream;
use syn::{parse_macro_input, Meta};

#[proc_macro_derive(FieldsHaveDefault)]
pub fn derive_fields_required(input: TokenStream) -> TokenStream {
    let data = parse_macro_input!(input as syn::DeriveInput);
    let trait_ident = syn::Ident::new("FieldsHaveDefault", proc_macro2::Span::call_site());
    let ident = &data.ident;
    let is_required = match data.data.clone() {
        syn::Data::Struct(syn::DataStruct { fields, .. }) => fields
            .into_iter()
            .map(|f| {
                f.attrs.into_iter().any(|a| {
                    a.path().is_ident("serde")
                        && match a.meta {
                            Meta::List(meta_list) => {
                                meta_list.tokens.into_iter().any(|t| match t {
                                    proc_macro2::TokenTree::Ident(ident) => {
                                        ident.to_string().trim().starts_with("default")
                                    }
                                    _ => false,
                                })
                            }
                            _ => false,
                        }
                })
            })
            .collect(),
        _ => {
            vec![]
        }
    };
    quote::quote! {
        #[automatically_derived]
        impl annatto_derive::#trait_ident for #ident {
            const FIELDS_HAVE_DEFAULT: &'static [bool] = &[#(#is_required), *];
        }
    }
    .into()
}
